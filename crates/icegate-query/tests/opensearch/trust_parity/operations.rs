//! trust's operation reads over `operations` joined to `prices_effective`: every operation of one
//! trace without its content columns, and a conversation thread over a trace-id list and a scan
//! window with its content columns.
//!
//! Costs follow trust's five-term formula (`tokens / 1e6 * rate`, cache and request rates
//! coalesced to 0, input and output rates strict), gated to `0.0` for an operation without a
//! request model. Tenant B writes an operation on A's trace `0c…` under a span id A does not
//! use, so a leaked row is always one row more.
#![allow(clippy::expect_used)]

use serde_json::{Map, Value, json};

use super::harness::{ParityServers, SECOND_MICROS, TENANT_A, TENANT_B, assert_rows_in_order};
use super::tables::{OPERATION_DEFAULTS, OperationLine, commit_operation_lines, commit_rate_card};

/// The single-trace read's trace.
const TRACE_C: [u8; 16] = [0x0c; 16];
/// The thread's second trace.
const TRACE_D: [u8; 16] = [0x0d; 16];
/// A trace the thread does not list.
const TRACE_E: [u8; 16] = [0x0e; 16];

/// Four operations of trace `0c…`, two of trace `0d…` (the second three days after the window),
/// one of trace `0e…`, and B's operation on trace `0c…` at +63 s under its own span id `cb…`.
static OPERATION_LINES: [OperationLine; 8] = [
    OperationLine {
        trace_id: TRACE_C,
        span_id: [0xc1; 8],
        conversation_id: Some("conv-1"),
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 60 * SECOND_MICROS,
        time_to_first_chunk_ms: Some(120),
        input_tokens: Some(1000),
        output_tokens: Some(500),
        total_tokens: Some(1500),
        cache_read_input_tokens: Some(200),
        user_id: Some("u-1"),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: TRACE_C,
        span_id: [0xc2; 8],
        operation_name: "execute_tool",
        start_offset_micros: 61 * SECOND_MICROS,
        duration_micros: 500_000,
        tool_name: Some("search"),
        tool_call_id: Some("call-1"),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: TRACE_C,
        span_id: [0xc3; 8],
        conversation_id: Some("conv-1"),
        provider_name: Some("anthropic"),
        request_model: Some("claude-x"),
        start_offset_micros: 62 * SECOND_MICROS,
        duration_micros: 2_000_000,
        input_tokens: Some(250_000),
        output_tokens: Some(1000),
        total_tokens: Some(251_000),
        cache_read_input_tokens: Some(0),
        cache_creation_input_tokens: Some(1000),
        user_id: Some("u-1"),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: TRACE_C,
        span_id: [0xc4; 8],
        conversation_id: Some("conv-1"),
        provider_name: Some("openai"),
        request_model: Some("gpt-mini"),
        start_offset_micros: 65 * SECOND_MICROS,
        input_tokens: Some(100),
        output_tokens: Some(50),
        total_tokens: Some(150),
        user_id: Some("u-1"),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: TRACE_D,
        span_id: [0xd1; 8],
        conversation_id: Some("conv-1"),
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 70 * SECOND_MICROS,
        input_tokens: Some(2000),
        output_tokens: Some(1000),
        total_tokens: Some(3000),
        user_id: Some("u-1"),
        input_messages: Some(r#"[{"role":"user","content":"hi"}]"#),
        output_messages: Some(r#"[{"role":"assistant","content":"hello"}]"#),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: TRACE_D,
        span_id: [0xd2; 8],
        operation_name: "execute_tool",
        start_offset_micros: 259_200 * SECOND_MICROS,
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: TRACE_E,
        span_id: [0xe1; 8],
        conversation_id: Some("conv-1"),
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 75 * SECOND_MICROS,
        input_tokens: Some(10),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        tenant: TENANT_B,
        trace_id: TRACE_C,
        span_id: [0xcb; 8],
        conversation_id: Some("conv-1"),
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 63 * SECOND_MICROS,
        input_tokens: Some(9999),
        output_tokens: Some(9999),
        total_tokens: Some(19_998),
        user_id: Some("u-b"),
        ..OPERATION_DEFAULTS
    },
];

/// The 33 columns of trust's operation projection, in its order.
const OPERATION_COLUMNS: [&str; 33] = [
    "span_id",
    "trace_id",
    "conversation_id",
    "parent_span_id",
    "operation_name",
    "timestamp",
    "end_timestamp",
    "duration_micros",
    "time_to_first_chunk_ms",
    "request_model",
    "response_model",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "reasoning_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "status_code",
    "status_message",
    "error_type",
    "tool_name",
    "tool_call_id",
    "service_name",
    "user_id",
    "input_messages",
    "output_messages",
    "system_instructions",
    "tool_call_arguments",
    "tool_call_result",
    "input_cost_usd",
    "output_cost_usd",
    "cost_usd",
    "unpriced",
];

/// An expected projection row: every column null except the ones `cells` states.
fn fill_operation_columns(cells: &Value) -> Value {
    let mut row: Map<String, Value> = OPERATION_COLUMNS
        .iter()
        .map(|column| ((*column).to_owned(), Value::Null))
        .collect();
    for (column, value) in cells.as_object().expect("an object of cells") {
        assert!(row.contains_key(column), "`{column}` is not a projection column");
        row.insert(column.clone(), value.clone());
    }
    Value::Object(row)
}

/// `gpt-4o` at the current revision `2.0` / `8.0` / cache read `1.0` (the orphaned `min 500`
/// tier matches too, and loses on `valid_from`): input `1000 * 2.0 / 1e6 + 200 * 1.0 / 1e6 =
/// 0.0022`, output `500 * 8.0 / 1e6 = 0.004`.
fn expect_chat_gpt_4o_row() -> Value {
    fill_operation_columns(&json!({
        "span_id": "c1c1c1c1c1c1c1c1", "trace_id": "0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c", "conversation_id": "conv-1",
        "operation_name": "chat", "timestamp": "2023-11-14 22:14:20", "end_timestamp": "2023-11-14 22:14:21",
        "duration_micros": 1_000_000, "time_to_first_chunk_ms": 120, "request_model": "gpt-4o",
        "input_tokens": 1000, "output_tokens": 500, "total_tokens": 1500, "cache_read_tokens": 200,
        "status_code": 0, "service_name": "agent", "user_id": "u-1",
        "input_cost_usd": 0.0022, "output_cost_usd": 0.004, "cost_usd": 0.0062, "unpriced": false
    }))
}

/// A tool call has no request model: every cost leg is `0.0` and it is not unpriced.
fn build_expected_tool_call_row() -> Value {
    fill_operation_columns(&json!({
        "span_id": "c2c2c2c2c2c2c2c2", "trace_id": "0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c",
        "operation_name": "execute_tool", "timestamp": "2023-11-14 22:14:21", "end_timestamp": "2023-11-14 22:14:21.5",
        "duration_micros": 500_000, "status_code": 0, "tool_name": "search", "tool_call_id": "call-1",
        "service_name": "agent", "input_cost_usd": 0.0, "output_cost_usd": 0.0, "cost_usd": 0.0, "unpriced": false
    }))
}

/// 250 000 input tokens select the `[200000, unbounded)` tier `6.0` / `22.5`, whose cache rates
/// are null and coalesce to 0: input `1.5`, output `0.0225`.
fn build_expected_chat_claude_long_context_row() -> Value {
    fill_operation_columns(&json!({
        "span_id": "c3c3c3c3c3c3c3c3", "trace_id": "0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c", "conversation_id": "conv-1",
        "operation_name": "chat", "timestamp": "2023-11-14 22:14:22", "end_timestamp": "2023-11-14 22:14:24",
        "duration_micros": 2_000_000, "request_model": "claude-x", "input_tokens": 250_000, "output_tokens": 1000,
        "total_tokens": 251_000, "cache_read_tokens": 0, "cache_write_tokens": 1000, "status_code": 0,
        "service_name": "agent", "user_id": "u-1",
        "input_cost_usd": 1.5, "output_cost_usd": 0.0225, "cost_usd": 1.5225, "unpriced": false
    }))
}

/// `gpt-mini` has no output rate: the output leg and the whole cost are null, the input leg is
/// `100 * 0.15 / 1e6`, and the operation is unpriced.
fn build_expected_chat_gpt_mini_row() -> Value {
    fill_operation_columns(&json!({
        "span_id": "c4c4c4c4c4c4c4c4", "trace_id": "0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c", "conversation_id": "conv-1",
        "operation_name": "chat", "timestamp": "2023-11-14 22:14:25", "end_timestamp": "2023-11-14 22:14:26",
        "duration_micros": 1_000_000, "request_model": "gpt-mini", "input_tokens": 100, "output_tokens": 50,
        "total_tokens": 150, "status_code": 0, "service_name": "agent", "user_id": "u-1",
        "input_cost_usd": 0.000_015, "output_cost_usd": null, "cost_usd": null, "unpriced": true
    }))
}

/// The thread's `gpt-4o` call with content: input `0.004`, output `0.008`.
fn build_expected_chat_with_content_row() -> Value {
    fill_operation_columns(&json!({
        "span_id": "d1d1d1d1d1d1d1d1", "trace_id": "0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d", "conversation_id": "conv-1",
        "operation_name": "chat", "timestamp": "2023-11-14 22:14:30", "end_timestamp": "2023-11-14 22:14:31",
        "duration_micros": 1_000_000, "request_model": "gpt-4o", "input_tokens": 2000, "output_tokens": 1000,
        "total_tokens": 3000, "status_code": 0, "service_name": "agent", "user_id": "u-1",
        "input_messages": r#"[{"role":"user","content":"hi"}]"#,
        "output_messages": r#"[{"role":"assistant","content":"hello"}]"#,
        "input_cost_usd": 0.004, "output_cost_usd": 0.008, "cost_usd": 0.012, "unpriced": false
    }))
}

/// `operations_by_traces_sql([0c…0c], None, 10000, false)`.
const SINGLE_TRACE_SQL: &str = r"WITH priced AS (SELECT o.*, p.input_usd_per_1m, p.output_usd_per_1m, p.cache_read_usd_per_1m, p.cache_write_usd_per_1m, p.request_usd, row_number() OVER (PARTITION BY o.trace_id, o.span_id ORDER BY p.valid_from DESC, p.min_input_tokens DESC) AS rn
  FROM (SELECT * FROM iceberg.icegate.operations WHERE trace_id IN (arrow_cast(decode('0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c', 'hex'), 'FixedSizeBinary(16)'))) o
  LEFT JOIN iceberg.icegate.prices_effective p ON p.provider = o.provider_name AND p.model = o.request_model AND p.service_tier = 'standard' AND p.region = 'global' AND (p.min_input_tokens IS NULL OR coalesce(o.input_tokens, 0) >= p.min_input_tokens) AND (p.max_input_tokens IS NULL OR coalesce(o.input_tokens, 0) < p.max_input_tokens) AND o.timestamp >= p.valid_from AND (p.valid_to IS NULL OR o.timestamp < p.valid_to))
SELECT o.span_id AS span_id, o.trace_id AS trace_id, o.conversation_id AS conversation_id, o.parent_span_id AS parent_span_id, o.operation_name AS operation_name, o.timestamp AS timestamp, o.end_timestamp AS end_timestamp, o.duration_micros AS duration_micros, o.time_to_first_chunk_ms AS time_to_first_chunk_ms, o.request_model AS request_model, o.response_model AS response_model, o.input_tokens AS input_tokens, o.output_tokens AS output_tokens, o.total_tokens AS total_tokens, o.reasoning_tokens AS reasoning_tokens, o.cache_read_input_tokens AS cache_read_tokens, o.cache_creation_input_tokens AS cache_write_tokens, o.status_code AS status_code, o.status_message AS status_message, o.error_type AS error_type, o.tool_name AS tool_name, o.tool_call_id AS tool_call_id, o.service_name AS service_name, o.user_id AS user_id,
  CAST(NULL AS VARCHAR) AS input_messages, CAST(NULL AS VARCHAR) AS output_messages, CAST(NULL AS VARCHAR) AS system_instructions, CAST(NULL AS VARCHAR) AS tool_call_arguments, CAST(NULL AS VARCHAR) AS tool_call_result,
  CASE WHEN o.request_model IS NULL THEN 0.0 ELSE coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0) END AS input_cost_usd,
  CASE WHEN o.request_model IS NULL THEN 0.0 ELSE coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m END AS output_cost_usd,
  CASE WHEN o.request_model IS NULL THEN 0.0 ELSE coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0) END AS cost_usd,
  (o.request_model IS NOT NULL AND (o.input_usd_per_1m IS NULL OR o.output_usd_per_1m IS NULL)) AS unpriced
FROM priced o WHERE o.rn = 1 ORDER BY o.timestamp ASC, o.span_id ASC LIMIT 10001";

const SINGLE_TRACE_PPL: &str = "source=operations \
    | where trace_id = '0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c' \
    | fields - input_messages, output_messages, system_instructions, tool_call_arguments, tool_call_result \
    | left join max=0 left=o right=p on o.provider_name = p.provider and o.request_model = p.model and (isnull(p.min_input_tokens) or coalesce(o.input_tokens, 0) >= p.min_input_tokens) and (isnull(p.max_input_tokens) or coalesce(o.input_tokens, 0) < p.max_input_tokens) and o.`timestamp` >= p.valid_from and (isnull(p.valid_to) or o.`timestamp` < p.valid_to) \
        [ source=prices_effective | where service_tier = 'standard' and region = 'global' | fields provider, model, min_input_tokens, max_input_tokens, valid_from, valid_to, input_usd_per_1m, output_usd_per_1m, cache_read_usd_per_1m, cache_write_usd_per_1m, request_usd ] \
    | sort - valid_from, - min_input_tokens \
    | dedup trace_id, span_id \
    | eval cache_read_tokens = cache_read_input_tokens, cache_write_tokens = cache_creation_input_tokens, input_messages = nullif('', ''), output_messages = nullif('', ''), system_instructions = nullif('', ''), tool_call_arguments = nullif('', ''), tool_call_result = nullif('', ''), \
        input_cost_usd = if(isnull(request_model), 0.0D, coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0)), \
        output_cost_usd = if(isnull(request_model), 0.0D, coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m), \
        cost_usd = if(isnull(request_model), 0.0D, coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0)), \
        unpriced = isnotnull(request_model) and (isnull(input_usd_per_1m) or isnull(output_usd_per_1m)) \
    | sort `timestamp`, span_id \
    | head 10001 \
    | fields span_id, trace_id, conversation_id, parent_span_id, operation_name, `timestamp`, end_timestamp, duration_micros, time_to_first_chunk_ms, request_model, response_model, input_tokens, output_tokens, total_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens, status_code, status_message, error_type, tool_name, tool_call_id, service_name, user_id, input_messages, output_messages, system_instructions, tool_call_arguments, tool_call_result, input_cost_usd, output_cost_usd, cost_usd, unpriced";

/// `operations_by_traces_sql([0c…0c, 0d…0d], Some(scan bounds), 1000, true)`. The scan bounds are
/// `scan_bounds` of the stamped extent `[22:14:20, 22:14:31]`: a day of slack on each side
/// rounded out to whole days, `[2023-11-13 00:00:00, 2023-11-16 00:00:00)`.
const THREAD_SQL: &str = r"WITH priced AS (SELECT o.*, p.input_usd_per_1m, p.output_usd_per_1m, p.cache_read_usd_per_1m, p.cache_write_usd_per_1m, p.request_usd, row_number() OVER (PARTITION BY o.trace_id, o.span_id ORDER BY p.valid_from DESC, p.min_input_tokens DESC) AS rn
  FROM (SELECT * FROM iceberg.icegate.operations WHERE trace_id IN (arrow_cast(decode('0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c', 'hex'), 'FixedSizeBinary(16)'), arrow_cast(decode('0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d', 'hex'), 'FixedSizeBinary(16)')) AND timestamp >= to_timestamp_micros(1699833600000000) AND timestamp < to_timestamp_micros(1700092800000000)) o
  LEFT JOIN iceberg.icegate.prices_effective p ON p.provider = o.provider_name AND p.model = o.request_model AND p.service_tier = 'standard' AND p.region = 'global' AND (p.min_input_tokens IS NULL OR coalesce(o.input_tokens, 0) >= p.min_input_tokens) AND (p.max_input_tokens IS NULL OR coalesce(o.input_tokens, 0) < p.max_input_tokens) AND o.timestamp >= p.valid_from AND (p.valid_to IS NULL OR o.timestamp < p.valid_to))
SELECT o.span_id AS span_id, o.trace_id AS trace_id, o.conversation_id AS conversation_id, o.parent_span_id AS parent_span_id, o.operation_name AS operation_name, o.timestamp AS timestamp, o.end_timestamp AS end_timestamp, o.duration_micros AS duration_micros, o.time_to_first_chunk_ms AS time_to_first_chunk_ms, o.request_model AS request_model, o.response_model AS response_model, o.input_tokens AS input_tokens, o.output_tokens AS output_tokens, o.total_tokens AS total_tokens, o.reasoning_tokens AS reasoning_tokens, o.cache_read_input_tokens AS cache_read_tokens, o.cache_creation_input_tokens AS cache_write_tokens, o.status_code AS status_code, o.status_message AS status_message, o.error_type AS error_type, o.tool_name AS tool_name, o.tool_call_id AS tool_call_id, o.service_name AS service_name, o.user_id AS user_id,
  o.input_messages AS input_messages, o.output_messages AS output_messages, o.system_instructions AS system_instructions, o.tool_call_arguments AS tool_call_arguments, o.tool_call_result AS tool_call_result,
  CASE WHEN o.request_model IS NULL THEN 0.0 ELSE coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0) END AS input_cost_usd,
  CASE WHEN o.request_model IS NULL THEN 0.0 ELSE coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m END AS output_cost_usd,
  CASE WHEN o.request_model IS NULL THEN 0.0 ELSE coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0) END AS cost_usd,
  (o.request_model IS NOT NULL AND (o.input_usd_per_1m IS NULL OR o.output_usd_per_1m IS NULL)) AS unpriced
FROM priced o WHERE o.rn = 1 ORDER BY o.timestamp ASC, o.span_id ASC LIMIT 1001";

const THREAD_PPL: &str = "source=operations \
    | where trace_id in ('0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c', '0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d') and `timestamp` >= timestamp('2023-11-13 00:00:00') and `timestamp` < timestamp('2023-11-16 00:00:00') \
    | left join max=0 left=o right=p on o.provider_name = p.provider and o.request_model = p.model and (isnull(p.min_input_tokens) or coalesce(o.input_tokens, 0) >= p.min_input_tokens) and (isnull(p.max_input_tokens) or coalesce(o.input_tokens, 0) < p.max_input_tokens) and o.`timestamp` >= p.valid_from and (isnull(p.valid_to) or o.`timestamp` < p.valid_to) \
        [ source=prices_effective | where service_tier = 'standard' and region = 'global' | fields provider, model, min_input_tokens, max_input_tokens, valid_from, valid_to, input_usd_per_1m, output_usd_per_1m, cache_read_usd_per_1m, cache_write_usd_per_1m, request_usd ] \
    | sort - valid_from, - min_input_tokens \
    | dedup trace_id, span_id \
    | eval cache_read_tokens = cache_read_input_tokens, cache_write_tokens = cache_creation_input_tokens, \
        input_cost_usd = if(isnull(request_model), 0.0D, coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0)), \
        output_cost_usd = if(isnull(request_model), 0.0D, coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m), \
        cost_usd = if(isnull(request_model), 0.0D, coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0)), \
        unpriced = isnotnull(request_model) and (isnull(input_usd_per_1m) or isnull(output_usd_per_1m)) \
    | sort `timestamp`, span_id \
    | head 1001 \
    | fields span_id, trace_id, conversation_id, parent_span_id, operation_name, `timestamp`, end_timestamp, duration_micros, time_to_first_chunk_ms, request_model, response_model, input_tokens, output_tokens, total_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens, status_code, status_message, error_type, tool_name, tool_call_id, service_name, user_id, input_messages, output_messages, system_instructions, tool_call_arguments, tool_call_result, input_cost_usd, output_cost_usd, cost_usd, unpriced";

/// A's four operations of trace `0c…`, chronological. B's operation on the same trace at +63 s
/// would be a fifth row, between the `claude-x` and the `gpt-mini` calls, if it leaked.
#[tokio::test]
async fn priced_operations_of_one_trace_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    commit_rate_card(&catalog).await;
    commit_operation_lines(&catalog, &OPERATION_LINES).await;
    let expected = [
        expect_chat_gpt_4o_row(),
        build_expected_tool_call_row(),
        build_expected_chat_claude_long_context_row(),
        build_expected_chat_gpt_mini_row(),
    ];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, SINGLE_TRACE_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, SINGLE_TRACE_PPL).await, &expected);
    servers.shutdown().await;
}

/// The thread reads both listed traces inside the scan window, content included: trace `0d…`'s
/// tool call three days later and trace `0e…` stay out. B's operation on trace `0c…` would be a
/// sixth row if it leaked.
#[tokio::test]
async fn thread_operations_over_a_trace_id_list_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    commit_rate_card(&catalog).await;
    commit_operation_lines(&catalog, &OPERATION_LINES).await;
    let expected = [
        expect_chat_gpt_4o_row(),
        build_expected_tool_call_row(),
        build_expected_chat_claude_long_context_row(),
        build_expected_chat_gpt_mini_row(),
        build_expected_chat_with_content_row(),
    ];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, THREAD_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, THREAD_PPL).await, &expected);
    servers.shutdown().await;
}
