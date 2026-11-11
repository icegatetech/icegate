//! trust's Conversations statements over `operations`: the list (unfiltered and with a trace-id
//! `HAVING`), the window summary, one conversation, its trace windows and the facets.
//!
//! The seed: conversation `conv-1` on trace `31…` with an unstamped tool call that the trace
//! closure reaches; a foreign `conv-x` operation on the same trace; `conv-2` on traces `32…` and
//! `33…`, the second also holding an operation two days after the window; `conv-3` two days
//! before the window; and tenant B's `conv-2` operation on trace `39…`.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use iceberg::Catalog;
use serde_json::{Value, json};

use super::harness::{
    ParityServers, SECOND_MICROS, TENANT_A, TENANT_B, assert_rows_in_order, build_facet_map, collect_ppl_facets,
    collect_sql_facets,
};
use super::tables::{OPERATION_DEFAULTS, OperationLine, commit_operation_lines, commit_rate_card};

/// The conversation seed, A's lines first.
static CONVERSATION_LINES: [OperationLine; 8] = [
    OperationLine {
        trace_id: [0x31; 16],
        span_id: [0x41; 8],
        conversation_id: Some("conv-1"),
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 100 * SECOND_MICROS,
        input_tokens: Some(1000),
        output_tokens: Some(500),
        total_tokens: Some(1500),
        user_id: Some("u-1"),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x31; 16],
        span_id: [0x42; 8],
        operation_name: "execute_tool",
        start_offset_micros: 101 * SECOND_MICROS,
        tool_name: Some("search"),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x31; 16],
        span_id: [0x43; 8],
        conversation_id: Some("conv-x"),
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 102 * SECOND_MICROS,
        duration_micros: 500_000,
        input_tokens: Some(10),
        output_tokens: Some(10),
        total_tokens: Some(20),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x32; 16],
        span_id: [0x44; 8],
        conversation_id: Some("conv-2"),
        provider_name: Some("anthropic"),
        request_model: Some("claude-x"),
        start_offset_micros: 200 * SECOND_MICROS,
        duration_micros: 3_000_000,
        input_tokens: Some(1000),
        output_tokens: Some(2000),
        user_id: Some("u-2"),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x33; 16],
        span_id: [0x45; 8],
        conversation_id: Some("conv-2"),
        provider_name: Some("openai"),
        request_model: Some("gpt-mini"),
        start_offset_micros: 300 * SECOND_MICROS,
        input_tokens: Some(100),
        output_tokens: Some(50),
        total_tokens: Some(150),
        user_id: Some("u-2"),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x34; 16],
        span_id: [0x46; 8],
        conversation_id: Some("conv-3"),
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: -172_800 * SECOND_MICROS,
        input_tokens: Some(10),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x33; 16],
        span_id: [0x47; 8],
        conversation_id: Some("conv-2"),
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 172_800 * SECOND_MICROS,
        input_tokens: Some(1000),
        output_tokens: Some(0),
        total_tokens: Some(1000),
        user_id: Some("u-2"),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        tenant: TENANT_B,
        trace_id: [0x39; 16],
        span_id: [0x49; 8],
        conversation_id: Some("conv-2"),
        provider_name: Some("b-provider"),
        request_model: Some("b-model"),
        start_offset_micros: 50 * SECOND_MICROS,
        input_tokens: Some(5000),
        output_tokens: Some(5000),
        total_tokens: Some(10_000),
        user_id: Some("u-b"),
        ..OPERATION_DEFAULTS
    },
];

/// `conv-2` inside the list's scan window: the claude call costs `1000 * 3.0 / 1e6 + 2000 *
/// 15.0 / 1e6 = 0.033` (no `total_tokens`, so `1000 + 2000` count), the `gpt-mini` call is
/// unpriced and adds no cost; the operation two days later is outside the scan window.
fn build_expected_conversation_two_windowed_row() -> Value {
    json!({"conversation_id": "conv-2", "started_at": "2023-11-14 22:16:40", "ended_at": "2023-11-14 22:18:21",
        "trace_count": 2, "total_tokens": 3150, "user_id": "u-2", "models": ["claude-x", "gpt-mini"],
        "cost_usd": 0.033, "unpriced_operations": 1})
}

/// `conv-x` reaches the unstamped tool call and its own call on trace `31…`, not `conv-1`'s call.
fn build_expected_conversation_x_row() -> Value {
    json!({"conversation_id": "conv-x", "started_at": "2023-11-14 22:15:01", "ended_at": "2023-11-14 22:15:02.5",
        "trace_count": 1, "total_tokens": 20, "user_id": null, "models": ["gpt-4o"],
        "cost_usd": 0.0001, "unpriced_operations": 0})
}

/// `conv-1`: its call (`0.002 + 0.004`) and the unstamped tool call, not `conv-x`'s call.
fn build_expected_conversation_one_row() -> Value {
    json!({"conversation_id": "conv-1", "started_at": "2023-11-14 22:15:00", "ended_at": "2023-11-14 22:15:02",
        "trace_count": 1, "total_tokens": 1500, "user_id": "u-1", "models": ["gpt-4o"],
        "cost_usd": 0.006, "unpriced_operations": 0})
}

/// `conv-2` whole: the find path has no window, so the call two days later (`0.002`) counts.
fn build_expected_conversation_two_whole_row() -> Value {
    json!({"conversation_id": "conv-2", "started_at": "2023-11-14 22:16:40", "ended_at": "2023-11-16 22:13:21",
        "trace_count": 2, "total_tokens": 4150, "user_id": "u-2", "models": ["claude-x", "gpt-4o", "gpt-mini"],
        "cost_usd": 0.035, "unpriced_operations": 1})
}

/// `list_conversations_sql(22:13:20, 23:13:20, 51, 0, None)`.
const LIST_SQL: &str = r"WITH conv_traces AS (SELECT DISTINCT conversation_id, trace_id FROM iceberg.icegate.operations WHERE conversation_id IS NOT NULL AND timestamp >= to_timestamp_micros(1700000000000000) AND timestamp < to_timestamp_micros(1700003600000000)),
priced AS (SELECT o.*, p.input_usd_per_1m, p.output_usd_per_1m, p.cache_read_usd_per_1m, p.cache_write_usd_per_1m, p.request_usd, row_number() OVER (PARTITION BY o.trace_id, o.span_id ORDER BY p.valid_from DESC, p.min_input_tokens DESC) AS rn
  FROM (SELECT * FROM iceberg.icegate.operations WHERE trace_id IN (SELECT trace_id FROM conv_traces) AND timestamp >= to_timestamp_micros(1699833600000000) AND timestamp < to_timestamp_micros(1700092800000000)) o
  LEFT JOIN iceberg.icegate.prices_effective p ON p.provider = o.provider_name AND p.model = o.request_model AND p.service_tier = 'standard' AND p.region = 'global' AND (p.min_input_tokens IS NULL OR coalesce(o.input_tokens, 0) >= p.min_input_tokens) AND (p.max_input_tokens IS NULL OR coalesce(o.input_tokens, 0) < p.max_input_tokens) AND o.timestamp >= p.valid_from AND (p.valid_to IS NULL OR o.timestamp < p.valid_to))
SELECT ct.conversation_id AS conversation_id, min(o.timestamp) AS started_at, max(o.end_timestamp) AS ended_at, count(DISTINCT o.trace_id) AS trace_count, sum(coalesce(o.total_tokens, coalesce(o.input_tokens, 0) + coalesce(o.output_tokens, 0))) AS total_tokens, max(o.user_id) AS user_id, array_agg(DISTINCT o.request_model ORDER BY o.request_model) FILTER (WHERE o.request_model IS NOT NULL) AS models, sum(coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0)) AS cost_usd, count(*) FILTER (WHERE o.request_model IS NOT NULL AND (o.input_usd_per_1m IS NULL OR o.output_usd_per_1m IS NULL)) AS unpriced_operations
FROM priced o JOIN conv_traces ct ON o.trace_id = ct.trace_id AND (o.conversation_id IS NULL OR o.conversation_id = ct.conversation_id)
WHERE o.rn = 1
GROUP BY ct.conversation_id
ORDER BY started_at DESC, conversation_id ASC LIMIT 51 OFFSET 0";

const LIST_PPL: &str = "source=operations \
    | where trace_id in [ source=operations | where isnotnull(conversation_id) and `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') | fields trace_id ] and `timestamp` >= timestamp('2023-11-13 00:00:00') and `timestamp` < timestamp('2023-11-16 00:00:00') \
    | left join max=0 left=o right=p on o.provider_name = p.provider and o.request_model = p.model and (isnull(p.min_input_tokens) or coalesce(o.input_tokens, 0) >= p.min_input_tokens) and (isnull(p.max_input_tokens) or coalesce(o.input_tokens, 0) < p.max_input_tokens) and o.`timestamp` >= p.valid_from and (isnull(p.valid_to) or o.`timestamp` < p.valid_to) \
        [ source=prices_effective | where service_tier = 'standard' and region = 'global' | fields provider, model, min_input_tokens, max_input_tokens, valid_from, valid_to, input_usd_per_1m, output_usd_per_1m, cache_read_usd_per_1m, cache_write_usd_per_1m, request_usd ] \
    | sort - valid_from, - min_input_tokens \
    | dedup trace_id, span_id \
    | inner join max=0 left=o right=ct on o.trace_id = ct.ct_trace_id and (isnull(o.conversation_id) or o.conversation_id = ct.ct_conversation_id) \
        [ source=operations | where isnotnull(conversation_id) and `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') | fields conversation_id, trace_id | dedup conversation_id, trace_id | rename conversation_id as ct_conversation_id, trace_id as ct_trace_id ] \
    | eval tokens = coalesce(total_tokens, coalesce(input_tokens, 0) + coalesce(output_tokens, 0)), cost = coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0), is_unpriced = isnotnull(request_model) and (isnull(input_usd_per_1m) or isnull(output_usd_per_1m)) \
    | stats min(`timestamp`) as started_at, max(end_timestamp) as ended_at, dc(trace_id) as trace_count, sum(tokens) as total_tokens, max(user_id) as user_id, values(request_model) as models, sum(cost) as cost_usd, count(eval(is_unpriced)) as unpriced_operations by ct_conversation_id \
    | rename ct_conversation_id as conversation_id \
    | sort - started_at, conversation_id \
    | head 51 from 0 \
    | fields conversation_id, started_at, ended_at, trace_count, total_tokens, user_id, models, cost_usd, unpriced_operations";

/// The list under the conversation filter leaf `trace_id = 3333…33` (`with_having`): an
/// existence test over the conversation's operations.
const LIST_TRACE_HAVING_SQL: &str = r"WITH conv_traces AS (SELECT DISTINCT conversation_id, trace_id FROM iceberg.icegate.operations WHERE conversation_id IS NOT NULL AND timestamp >= to_timestamp_micros(1700000000000000) AND timestamp < to_timestamp_micros(1700003600000000)),
priced AS (SELECT o.*, p.input_usd_per_1m, p.output_usd_per_1m, p.cache_read_usd_per_1m, p.cache_write_usd_per_1m, p.request_usd, row_number() OVER (PARTITION BY o.trace_id, o.span_id ORDER BY p.valid_from DESC, p.min_input_tokens DESC) AS rn
  FROM (SELECT * FROM iceberg.icegate.operations WHERE trace_id IN (SELECT trace_id FROM conv_traces) AND timestamp >= to_timestamp_micros(1699833600000000) AND timestamp < to_timestamp_micros(1700092800000000)) o
  LEFT JOIN iceberg.icegate.prices_effective p ON p.provider = o.provider_name AND p.model = o.request_model AND p.service_tier = 'standard' AND p.region = 'global' AND (p.min_input_tokens IS NULL OR coalesce(o.input_tokens, 0) >= p.min_input_tokens) AND (p.max_input_tokens IS NULL OR coalesce(o.input_tokens, 0) < p.max_input_tokens) AND o.timestamp >= p.valid_from AND (p.valid_to IS NULL OR o.timestamp < p.valid_to))
SELECT ct.conversation_id AS conversation_id, min(o.timestamp) AS started_at, max(o.end_timestamp) AS ended_at, count(DISTINCT o.trace_id) AS trace_count, sum(coalesce(o.total_tokens, coalesce(o.input_tokens, 0) + coalesce(o.output_tokens, 0))) AS total_tokens, max(o.user_id) AS user_id, array_agg(DISTINCT o.request_model ORDER BY o.request_model) FILTER (WHERE o.request_model IS NOT NULL) AS models, sum(coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0)) AS cost_usd, count(*) FILTER (WHERE o.request_model IS NOT NULL AND (o.input_usd_per_1m IS NULL OR o.output_usd_per_1m IS NULL)) AS unpriced_operations
FROM priced o JOIN conv_traces ct ON o.trace_id = ct.trace_id AND (o.conversation_id IS NULL OR o.conversation_id = ct.conversation_id)
WHERE o.rn = 1
GROUP BY ct.conversation_id HAVING bool_or(o.trace_id = arrow_cast(decode('33333333333333333333333333333333', 'hex'), 'FixedSizeBinary(16)'))
ORDER BY started_at DESC, conversation_id ASC LIMIT 51 OFFSET 0";

const LIST_TRACE_HAVING_PPL: &str = "source=operations \
    | where trace_id in [ source=operations | where isnotnull(conversation_id) and `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') | fields trace_id ] and `timestamp` >= timestamp('2023-11-13 00:00:00') and `timestamp` < timestamp('2023-11-16 00:00:00') \
    | left join max=0 left=o right=p on o.provider_name = p.provider and o.request_model = p.model and (isnull(p.min_input_tokens) or coalesce(o.input_tokens, 0) >= p.min_input_tokens) and (isnull(p.max_input_tokens) or coalesce(o.input_tokens, 0) < p.max_input_tokens) and o.`timestamp` >= p.valid_from and (isnull(p.valid_to) or o.`timestamp` < p.valid_to) \
        [ source=prices_effective | where service_tier = 'standard' and region = 'global' | fields provider, model, min_input_tokens, max_input_tokens, valid_from, valid_to, input_usd_per_1m, output_usd_per_1m, cache_read_usd_per_1m, cache_write_usd_per_1m, request_usd ] \
    | sort - valid_from, - min_input_tokens \
    | dedup trace_id, span_id \
    | inner join max=0 left=o right=ct on o.trace_id = ct.ct_trace_id and (isnull(o.conversation_id) or o.conversation_id = ct.ct_conversation_id) \
        [ source=operations | where isnotnull(conversation_id) and `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') | fields conversation_id, trace_id | dedup conversation_id, trace_id | rename conversation_id as ct_conversation_id, trace_id as ct_trace_id ] \
    | eval tokens = coalesce(total_tokens, coalesce(input_tokens, 0) + coalesce(output_tokens, 0)), cost = coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0), is_unpriced = isnotnull(request_model) and (isnull(input_usd_per_1m) or isnull(output_usd_per_1m)) \
    | stats min(`timestamp`) as started_at, max(end_timestamp) as ended_at, dc(trace_id) as trace_count, sum(tokens) as total_tokens, max(user_id) as user_id, values(request_model) as models, sum(cost) as cost_usd, count(eval(is_unpriced)) as unpriced_operations, count(eval(trace_id = '33333333333333333333333333333333')) as matched_trace by ct_conversation_id \
    | where matched_trace > 0 \
    | rename ct_conversation_id as conversation_id \
    | sort - started_at, conversation_id \
    | head 51 from 0 \
    | fields conversation_id, started_at, ended_at, trace_count, total_tokens, user_id, models, cost_usd, unpriced_operations";

/// `summary_conversations_sql(22:13:20, 23:13:20, None)`.
const SUMMARY_SQL: &str = r"WITH conv_traces AS (SELECT DISTINCT conversation_id, trace_id FROM iceberg.icegate.operations WHERE conversation_id IS NOT NULL AND timestamp >= to_timestamp_micros(1700000000000000) AND timestamp < to_timestamp_micros(1700003600000000)),
priced AS (SELECT o.*, p.input_usd_per_1m, p.output_usd_per_1m, p.cache_read_usd_per_1m, p.cache_write_usd_per_1m, p.request_usd, row_number() OVER (PARTITION BY o.trace_id, o.span_id ORDER BY p.valid_from DESC, p.min_input_tokens DESC) AS rn
  FROM (SELECT * FROM iceberg.icegate.operations WHERE trace_id IN (SELECT trace_id FROM conv_traces) AND timestamp >= to_timestamp_micros(1699833600000000) AND timestamp < to_timestamp_micros(1700092800000000)) o
  LEFT JOIN iceberg.icegate.prices_effective p ON p.provider = o.provider_name AND p.model = o.request_model AND p.service_tier = 'standard' AND p.region = 'global' AND (p.min_input_tokens IS NULL OR coalesce(o.input_tokens, 0) >= p.min_input_tokens) AND (p.max_input_tokens IS NULL OR coalesce(o.input_tokens, 0) < p.max_input_tokens) AND o.timestamp >= p.valid_from AND (p.valid_to IS NULL OR o.timestamp < p.valid_to)),
per_conv AS (SELECT ct.conversation_id AS conversation_id, sum(coalesce(o.total_tokens, coalesce(o.input_tokens, 0) + coalesce(o.output_tokens, 0))) AS total_tokens, sum(coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0)) AS cost_usd, count(*) FILTER (WHERE o.request_model IS NOT NULL AND (o.input_usd_per_1m IS NULL OR o.output_usd_per_1m IS NULL)) AS unpriced_operations
  FROM priced o JOIN conv_traces ct ON o.trace_id = ct.trace_id AND (o.conversation_id IS NULL OR o.conversation_id = ct.conversation_id)
  WHERE o.rn = 1
  GROUP BY ct.conversation_id)
SELECT count(*) AS conversation_count, coalesce(sum(total_tokens), 0) AS total_tokens, coalesce(sum(cost_usd), 0) AS total_cost_usd, coalesce(sum(unpriced_operations), 0) AS unpriced_operations FROM per_conv";

const SUMMARY_PPL: &str = "source=operations \
    | where trace_id in [ source=operations | where isnotnull(conversation_id) and `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') | fields trace_id ] and `timestamp` >= timestamp('2023-11-13 00:00:00') and `timestamp` < timestamp('2023-11-16 00:00:00') \
    | left join max=0 left=o right=p on o.provider_name = p.provider and o.request_model = p.model and (isnull(p.min_input_tokens) or coalesce(o.input_tokens, 0) >= p.min_input_tokens) and (isnull(p.max_input_tokens) or coalesce(o.input_tokens, 0) < p.max_input_tokens) and o.`timestamp` >= p.valid_from and (isnull(p.valid_to) or o.`timestamp` < p.valid_to) \
        [ source=prices_effective | where service_tier = 'standard' and region = 'global' | fields provider, model, min_input_tokens, max_input_tokens, valid_from, valid_to, input_usd_per_1m, output_usd_per_1m, cache_read_usd_per_1m, cache_write_usd_per_1m, request_usd ] \
    | sort - valid_from, - min_input_tokens \
    | dedup trace_id, span_id \
    | inner join max=0 left=o right=ct on o.trace_id = ct.ct_trace_id and (isnull(o.conversation_id) or o.conversation_id = ct.ct_conversation_id) \
        [ source=operations | where isnotnull(conversation_id) and `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') | fields conversation_id, trace_id | dedup conversation_id, trace_id | rename conversation_id as ct_conversation_id, trace_id as ct_trace_id ] \
    | eval tokens = coalesce(total_tokens, coalesce(input_tokens, 0) + coalesce(output_tokens, 0)), cost = coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0), is_unpriced = isnotnull(request_model) and (isnull(input_usd_per_1m) or isnull(output_usd_per_1m)) \
    | stats sum(tokens) as total_tokens, sum(cost) as cost_usd, count(eval(is_unpriced)) as unpriced_operations by ct_conversation_id \
    | stats count() as conversation_count, sum(total_tokens) as total_tokens, sum(cost_usd) as total_cost_usd, sum(unpriced_operations) as unpriced_operations \
    | eval total_tokens = coalesce(total_tokens, 0), total_cost_usd = coalesce(total_cost_usd, 0.0D), unpriced_operations = coalesce(unpriced_operations, 0)";

/// `find_conversation_sql('conv-2')`: the trace closure whole, no window, no paging.
const FIND_SQL: &str = r"WITH conv_traces AS (SELECT DISTINCT conversation_id, trace_id FROM iceberg.icegate.operations WHERE conversation_id = 'conv-2'),
priced AS (SELECT o.*, p.input_usd_per_1m, p.output_usd_per_1m, p.cache_read_usd_per_1m, p.cache_write_usd_per_1m, p.request_usd, row_number() OVER (PARTITION BY o.trace_id, o.span_id ORDER BY p.valid_from DESC, p.min_input_tokens DESC) AS rn
  FROM (SELECT * FROM iceberg.icegate.operations WHERE trace_id IN (SELECT trace_id FROM conv_traces)) o
  LEFT JOIN iceberg.icegate.prices_effective p ON p.provider = o.provider_name AND p.model = o.request_model AND p.service_tier = 'standard' AND p.region = 'global' AND (p.min_input_tokens IS NULL OR coalesce(o.input_tokens, 0) >= p.min_input_tokens) AND (p.max_input_tokens IS NULL OR coalesce(o.input_tokens, 0) < p.max_input_tokens) AND o.timestamp >= p.valid_from AND (p.valid_to IS NULL OR o.timestamp < p.valid_to))
SELECT ct.conversation_id AS conversation_id, min(o.timestamp) AS started_at, max(o.end_timestamp) AS ended_at, count(DISTINCT o.trace_id) AS trace_count, sum(coalesce(o.total_tokens, coalesce(o.input_tokens, 0) + coalesce(o.output_tokens, 0))) AS total_tokens, max(o.user_id) AS user_id, array_agg(DISTINCT o.request_model ORDER BY o.request_model) FILTER (WHERE o.request_model IS NOT NULL) AS models, sum(coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0)) AS cost_usd, count(*) FILTER (WHERE o.request_model IS NOT NULL AND (o.input_usd_per_1m IS NULL OR o.output_usd_per_1m IS NULL)) AS unpriced_operations
FROM priced o JOIN conv_traces ct ON o.trace_id = ct.trace_id AND (o.conversation_id IS NULL OR o.conversation_id = ct.conversation_id)
WHERE o.rn = 1
GROUP BY ct.conversation_id
ORDER BY started_at DESC, conversation_id ASC";

const FIND_PPL: &str = "source=operations \
    | where trace_id in [ source=operations | where conversation_id = 'conv-2' | fields trace_id ] \
    | left join max=0 left=o right=p on o.provider_name = p.provider and o.request_model = p.model and (isnull(p.min_input_tokens) or coalesce(o.input_tokens, 0) >= p.min_input_tokens) and (isnull(p.max_input_tokens) or coalesce(o.input_tokens, 0) < p.max_input_tokens) and o.`timestamp` >= p.valid_from and (isnull(p.valid_to) or o.`timestamp` < p.valid_to) \
        [ source=prices_effective | where service_tier = 'standard' and region = 'global' | fields provider, model, min_input_tokens, max_input_tokens, valid_from, valid_to, input_usd_per_1m, output_usd_per_1m, cache_read_usd_per_1m, cache_write_usd_per_1m, request_usd ] \
    | sort - valid_from, - min_input_tokens \
    | dedup trace_id, span_id \
    | inner join max=0 left=o right=ct on o.trace_id = ct.ct_trace_id and (isnull(o.conversation_id) or o.conversation_id = ct.ct_conversation_id) \
        [ source=operations | where conversation_id = 'conv-2' | fields conversation_id, trace_id | dedup conversation_id, trace_id | rename conversation_id as ct_conversation_id, trace_id as ct_trace_id ] \
    | eval tokens = coalesce(total_tokens, coalesce(input_tokens, 0) + coalesce(output_tokens, 0)), cost = coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0), is_unpriced = isnotnull(request_model) and (isnull(input_usd_per_1m) or isnull(output_usd_per_1m)) \
    | stats min(`timestamp`) as started_at, max(end_timestamp) as ended_at, dc(trace_id) as trace_count, sum(tokens) as total_tokens, max(user_id) as user_id, values(request_model) as models, sum(cost) as cost_usd, count(eval(is_unpriced)) as unpriced_operations by ct_conversation_id \
    | rename ct_conversation_id as conversation_id \
    | sort - started_at, conversation_id \
    | fields conversation_id, started_at, ended_at, trace_count, total_tokens, user_id, models, cost_usd, unpriced_operations";

/// `conversation_trace_windows_sql('conv-2')`.
const TRACE_WINDOWS_SQL: &str = "SELECT trace_id, min(timestamp) AS lo, max(end_timestamp) AS hi \
    FROM iceberg.icegate.operations WHERE conversation_id = 'conv-2' GROUP BY trace_id ORDER BY lo ASC LIMIT 1001";

const TRACE_WINDOWS_PPL: &str = "source=operations \
    | where conversation_id = 'conv-2' \
    | stats min(`timestamp`) as lo, max(end_timestamp) as hi by trace_id \
    | sort lo \
    | head 1001 \
    | fields trace_id, lo, hi";

/// `facets_sql(window, {request_model, provider})`: the operations of the traces stamped inside
/// the strict window, read within the widened scan window.
const FACETS_SQL: &str = r#"WITH src AS (SELECT nullif(request_model, '') AS "v0", nullif(provider_name, '') AS "v1" FROM "iceberg"."icegate"."operations" WHERE (timestamp >= to_timestamp_micros(1699833600000000)) AND (timestamp < to_timestamp_micros(1700092800000000)) AND (trace_id IN (SELECT trace_id FROM iceberg.icegate.operations WHERE conversation_id IS NOT NULL AND timestamp >= to_timestamp_micros(1700000000000000) AND timestamp < to_timestamp_micros(1700003600000000)))),
g AS (SELECT v0, v1 FROM src GROUP BY GROUPING SETS ((v0), (v1))),
u AS (SELECT CASE WHEN v0 IS NOT NULL THEN 'request_model' WHEN v1 IS NOT NULL THEN 'provider' END AS field, coalesce(v0, v1) AS value FROM g)
SELECT field, value FROM (SELECT field, value, row_number() OVER (PARTITION BY field ORDER BY value) AS rn FROM u WHERE value IS NOT NULL) t WHERE rn <= 201"#;

const FACETS_PPL: &str = "source=operations \
    | where `timestamp` >= timestamp('2023-11-13 00:00:00') and `timestamp` < timestamp('2023-11-16 00:00:00') and trace_id in [ source=operations | where isnotnull(conversation_id) and `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') | fields trace_id ] \
    | eval v0 = nullif(request_model, ''), v1 = nullif(provider_name, '') \
    | stats values(v0) as v0, values(v1) as v1 \
    | eval v0 = mvindex(v0, 0, 200), v1 = mvindex(v1, 0, 200)";

/// Write the rate card and the conversation seed.
async fn seed_conversations(catalog: &Arc<dyn Catalog>) {
    commit_rate_card(catalog).await;
    commit_operation_lines(catalog, &CONVERSATION_LINES).await;
}

/// Newest first: `conv-2`, `conv-x`, `conv-1`. `conv-3` was stamped outside the window; B's
/// `conv-2` operation would add a trace and 10 000 tokens to A's `conv-2` if it leaked.
#[tokio::test]
async fn conversation_list_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_conversations(&catalog).await;
    let expected = [
        build_expected_conversation_two_windowed_row(),
        build_expected_conversation_x_row(),
        build_expected_conversation_one_row(),
    ];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, LIST_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, LIST_PPL).await, &expected);
    servers.shutdown().await;
}

/// Only `conv-2` holds an operation on trace `33…`.
#[tokio::test]
async fn conversation_list_with_a_trace_id_having_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_conversations(&catalog).await;
    let expected = [build_expected_conversation_two_windowed_row()];
    assert_rows_in_order(
        "SQL",
        &servers.run_sql(TENANT_A, LIST_TRACE_HAVING_SQL).await,
        &expected,
    );
    assert_rows_in_order(
        "PPL",
        &servers.run_ppl(TENANT_A, LIST_TRACE_HAVING_PPL).await,
        &expected,
    );
    servers.shutdown().await;
}

/// Three conversations: `1500 + 20 + 3150` tokens, `0.006 + 0.0001 + 0.033` USD, one unpriced
/// operation.
#[tokio::test]
async fn conversation_summary_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_conversations(&catalog).await;
    let expected =
        [json!({"conversation_count": 3, "total_tokens": 4670, "total_cost_usd": 0.0391, "unpriced_operations": 1})];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, SUMMARY_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, SUMMARY_PPL).await, &expected);
    servers.shutdown().await;
}

/// The whole of `conv-2`, the call two days after the window included.
#[tokio::test]
async fn single_conversation_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_conversations(&catalog).await;
    let expected = [build_expected_conversation_two_whole_row()];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, FIND_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, FIND_PPL).await, &expected);
    servers.shutdown().await;
}

/// `conv-2`'s traces by first stamped start; B's `conv-2` trace `39…` starts earlier and stays out.
#[tokio::test]
async fn conversation_trace_windows_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_conversations(&catalog).await;
    let expected = [
        json!({"trace_id": "32323232323232323232323232323232", "lo": "2023-11-14 22:16:40", "hi": "2023-11-14 22:16:43"}),
        json!({"trace_id": "33333333333333333333333333333333", "lo": "2023-11-14 22:18:20", "hi": "2023-11-16 22:13:21"}),
    ];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, TRACE_WINDOWS_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, TRACE_WINDOWS_PPL).await, &expected);
    servers.shutdown().await;
}

/// Models and providers of the stamped traces' operations inside the scan window; B's
/// `b-model` / `b-provider` never appear.
#[tokio::test]
async fn conversation_facets_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_conversations(&catalog).await;
    let expected = build_facet_map(&[
        ("provider", &["anthropic", "openai"][..]),
        ("request_model", &["claude-x", "gpt-4o", "gpt-mini"][..]),
    ]);
    assert_eq!(
        collect_sql_facets(&servers.run_sql(TENANT_A, FACETS_SQL).await),
        expected,
        "SQL"
    );
    let ppl_rows = servers.run_ppl(TENANT_A, FACETS_PPL).await;
    assert_eq!(
        collect_ppl_facets(&ppl_rows, &[("v0", "request_model"), ("v1", "provider")]),
        expected,
        "PPL"
    );
    servers.shutdown().await;
}
