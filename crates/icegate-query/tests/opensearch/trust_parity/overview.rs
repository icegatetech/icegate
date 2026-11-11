//! trust's Overview statements over `operations`: KPI counts, latency percentiles per kind, the
//! window cost, the four buckets anchored at the window end, and the raw LLM durations.
//!
//! The bucket statements anchor `date_bin` at the window end; the PPL forms return the bucket
//! index `idx = (window_end - timestamp - 1 µs) / step`, which trust maps back to the bucket
//! start `window_end - (idx + 1) * step`. Both answers are compared as bucket starts in
//! microseconds (`bucket_us`).
#![allow(clippy::expect_used)]

use std::sync::Arc;

use chrono::NaiveDateTime;
use iceberg::Catalog;
use serde_json::{Value, json};

use super::harness::{
    ParityServers, SECOND_MICROS, TENANT_A, TENANT_B, WINDOW_START_MICROS, assert_rows_in_order, assert_rows_unordered,
};
use super::tables::{OPERATION_DEFAULTS, OperationLine, commit_operation_lines, commit_rate_card};

/// The window end, `2023-11-14 23:13:20`: the anchor of every bucket.
const WINDOW_END_MICROS: i64 = WINDOW_START_MICROS + 3_600 * SECOND_MICROS;
/// The bucket width trust renders the timeseries with.
const STEP_MICROS: i64 = 60 * SECOND_MICROS;

/// Five operations of A inside the window, one of them exactly at its inclusive start; three of
/// A outside it: one second before the start, exactly at the exclusive end, and after the end;
/// one of B inside it.
static OVERVIEW_LINES: [OperationLine; 9] = [
    OperationLine {
        trace_id: [0x51; 16],
        span_id: [0x57; 8],
        start_offset_micros: 0,
        duration_micros: 2_500_000,
        status_code: Some(2),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x51; 16],
        span_id: [0x51; 8],
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 30 * SECOND_MICROS,
        duration_micros: 1_000_000,
        time_to_first_chunk_ms: Some(100),
        input_tokens: Some(1000),
        output_tokens: Some(500),
        total_tokens: Some(1500),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x51; 16],
        span_id: [0x52; 8],
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 90 * SECOND_MICROS,
        duration_micros: 3_000_000,
        time_to_first_chunk_ms: Some(300),
        status_code: Some(2),
        input_tokens: Some(2000),
        output_tokens: Some(1000),
        total_tokens: Some(3000),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x51; 16],
        span_id: [0x53; 8],
        operation_name: "execute_tool",
        start_offset_micros: 150 * SECOND_MICROS,
        duration_micros: 200_000,
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x51; 16],
        span_id: [0x54; 8],
        operation_name: "embeddings",
        provider_name: Some("openai"),
        request_model: Some("text-embed"),
        start_offset_micros: 210 * SECOND_MICROS,
        duration_micros: 50_000,
        status_code: Some(2),
        input_tokens: Some(10),
        total_tokens: Some(10),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x51; 16],
        span_id: [0x55; 8],
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 3_700 * SECOND_MICROS,
        duration_micros: 9_000_000,
        status_code: Some(2),
        input_tokens: Some(1),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x51; 16],
        span_id: [0x58; 8],
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: -SECOND_MICROS,
        duration_micros: 6_000_000,
        status_code: Some(2),
        input_tokens: Some(1_000_000),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        trace_id: [0x51; 16],
        span_id: [0x59; 8],
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 3_600 * SECOND_MICROS,
        duration_micros: 7_000_000,
        status_code: Some(2),
        input_tokens: Some(1_000_000),
        ..OPERATION_DEFAULTS
    },
    OperationLine {
        tenant: TENANT_B,
        trace_id: [0x56; 16],
        span_id: [0x56; 8],
        provider_name: Some("openai"),
        request_model: Some("gpt-4o"),
        start_offset_micros: 30 * SECOND_MICROS,
        duration_micros: 99_000_000,
        status_code: Some(2),
        input_tokens: Some(999_999),
        ..OPERATION_DEFAULTS
    },
];

/// Write the rate card and the overview seed.
async fn seed_overview(catalog: &Arc<dyn Catalog>) {
    commit_rate_card(catalog).await;
    commit_operation_lines(catalog, &OVERVIEW_LINES).await;
}

/// 101 `chat` / `text_completion` operations of 1000 to 2000 ms (step 10 ms) with a first chunk
/// after 100 to 200 ms, one tool call of 200 ms, one embedding of 50 ms, one agent invocation
/// of 400 ms (first chunk 40 ms), all of A inside the window; and one 99 s `reranker` call of B,
/// a kind A has no row of.
fn build_latency_lines() -> Vec<OperationLine> {
    let mut lines: Vec<OperationLine> = (0..=100_u16)
        .map(|step| OperationLine {
            trace_id: [0x61; 16],
            span_id: u64::from(step + 1).to_be_bytes(),
            operation_name: if step % 2 == 0 { "chat" } else { "text_completion" },
            start_offset_micros: i64::from(step) * SECOND_MICROS,
            duration_micros: 1_000_000 + i64::from(step) * 10_000,
            time_to_first_chunk_ms: Some(100 + i64::from(step)),
            ..OPERATION_DEFAULTS
        })
        .collect();
    lines.extend([
        OperationLine {
            trace_id: [0x62; 16],
            span_id: [0x62; 8],
            operation_name: "execute_tool",
            start_offset_micros: 200 * SECOND_MICROS,
            duration_micros: 200_000,
            ..OPERATION_DEFAULTS
        },
        OperationLine {
            trace_id: [0x63; 16],
            span_id: [0x63; 8],
            operation_name: "embeddings",
            start_offset_micros: 201 * SECOND_MICROS,
            duration_micros: 50_000,
            ..OPERATION_DEFAULTS
        },
        OperationLine {
            trace_id: [0x64; 16],
            span_id: [0x64; 8],
            operation_name: "invoke_agent",
            start_offset_micros: 202 * SECOND_MICROS,
            duration_micros: 400_000,
            time_to_first_chunk_ms: Some(40),
            ..OPERATION_DEFAULTS
        },
        OperationLine {
            tenant: TENANT_B,
            trace_id: [0x65; 16],
            span_id: [0x65; 8],
            operation_name: "reranker",
            start_offset_micros: 203 * SECOND_MICROS,
            duration_micros: 99_000_000,
            time_to_first_chunk_ms: Some(9_999),
            ..OPERATION_DEFAULTS
        },
    ]);
    lines
}

/// `counts_sql(window)`.
const COUNTS_SQL: &str = r#"SELECT count(*) AS "events", sum(case when status_code = 2 then 1 else 0 end) AS "errored" FROM "iceberg"."icegate"."operations" WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000))"#;

const COUNTS_PPL: &str = "source=operations \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | stats count() as events, count(eval(status_code = 2)) as errored";

/// `kind_latency_sql(window, [0.5, 0.95])`.
const KIND_LATENCY_SQL: &str = r#"SELECT case when operation_name in ('chat','text_completion','generate_content') then 'llm' when operation_name = 'execute_tool' then 'tool' when operation_name in ('retrieval','reranker') then 'retrieval' when operation_name = 'embeddings' then 'embeddings' when operation_name in ('create_agent','invoke_agent') then 'agent' else 'other' end AS "kind", approx_percentile_cont(duration_micros / 1000.0, 0.5) AS "dur_p0", approx_percentile_cont(cast(time_to_first_chunk_ms as double), 0.5) AS "ttft_p0", approx_percentile_cont(duration_micros / 1000.0, 0.95) AS "dur_p1", approx_percentile_cont(cast(time_to_first_chunk_ms as double), 0.95) AS "ttft_p1"
FROM "iceberg"."icegate"."operations"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000))
GROUP BY case when operation_name in ('chat','text_completion','generate_content') then 'llm' when operation_name = 'execute_tool' then 'tool' when operation_name in ('retrieval','reranker') then 'retrieval' when operation_name = 'embeddings' then 'embeddings' when operation_name in ('create_agent','invoke_agent') then 'agent' else 'other' end"#;

/// The percentiles 0.5 and 0.95 rendered with the decimal point shifted (`50`, `95`).
const KIND_LATENCY_PPL: &str = "source=operations \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | eval kind = case(operation_name in ('chat', 'text_completion', 'generate_content'), 'llm', operation_name = 'execute_tool', 'tool', operation_name in ('retrieval', 'reranker'), 'retrieval', operation_name = 'embeddings', 'embeddings', operation_name in ('create_agent', 'invoke_agent'), 'agent' else 'other'), dur_ms = duration_micros / 1000.0D, ttft = cast(time_to_first_chunk_ms as double) \
    | stats percentile(dur_ms, 50) as dur_p0, percentile(ttft, 50) as ttft_p0, percentile(dur_ms, 95) as dur_p1, percentile(ttft, 95) as ttft_p1 by kind";

/// `window_cost_sql(window)`.
const WINDOW_COST_SQL: &str = r#"WITH priced AS (SELECT o.*, p.input_usd_per_1m, p.output_usd_per_1m, p.cache_read_usd_per_1m, p.cache_write_usd_per_1m, p.request_usd, row_number() OVER (PARTITION BY o.trace_id, o.span_id ORDER BY p.valid_from DESC, p.min_input_tokens DESC) AS rn
  FROM (SELECT * FROM iceberg.icegate.operations WHERE timestamp >= to_timestamp_micros(1700000000000000) AND timestamp < to_timestamp_micros(1700003600000000)) o
  LEFT JOIN iceberg.icegate.prices_effective p ON p.provider = o.provider_name AND p.model = o.request_model AND p.service_tier = 'standard' AND p.region = 'global' AND (p.min_input_tokens IS NULL OR coalesce(o.input_tokens, 0) >= p.min_input_tokens) AND (p.max_input_tokens IS NULL OR coalesce(o.input_tokens, 0) < p.max_input_tokens) AND o.timestamp >= p.valid_from AND (p.valid_to IS NULL OR o.timestamp < p.valid_to))
SELECT coalesce(sum(coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0)), 0) AS "cost_usd", count(*) FILTER (WHERE o.request_model IS NOT NULL AND (o.input_usd_per_1m IS NULL OR o.output_usd_per_1m IS NULL)) AS "unpriced_operations"
FROM "priced" AS "o"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000)) AND (o.rn = 1)"#;

const WINDOW_COST_PPL: &str = "source=operations \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | left join max=0 left=o right=p on o.provider_name = p.provider and o.request_model = p.model and (isnull(p.min_input_tokens) or coalesce(o.input_tokens, 0) >= p.min_input_tokens) and (isnull(p.max_input_tokens) or coalesce(o.input_tokens, 0) < p.max_input_tokens) and o.`timestamp` >= p.valid_from and (isnull(p.valid_to) or o.`timestamp` < p.valid_to) \
        [ source=prices_effective | where service_tier = 'standard' and region = 'global' | fields provider, model, min_input_tokens, max_input_tokens, valid_from, valid_to, input_usd_per_1m, output_usd_per_1m, cache_read_usd_per_1m, cache_write_usd_per_1m, request_usd ] \
    | sort - valid_from, - min_input_tokens \
    | dedup trace_id, span_id \
    | eval cost = coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0), is_unpriced = isnotnull(request_model) and (isnull(input_usd_per_1m) or isnull(output_usd_per_1m)) \
    | stats sum(cost) as cost_usd, count(eval(is_unpriced)) as unpriced_operations \
    | eval cost_usd = coalesce(cost_usd, 0.0D)";

/// `latency_timeseries_sql(window, 60 s, [0.5])`.
const LATENCY_SERIES_SQL: &str = r#"SELECT "request_model", arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)') AS "bucket", approx_percentile_cont(duration_micros / 1000.0, 0.5) AS "p0"
FROM "iceberg"."icegate"."operations"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000)) AND (request_model is not null)
GROUP BY "request_model", arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)')
ORDER BY arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)') ASC"#;

const LATENCY_SERIES_PPL: &str = "source=operations \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') and isnotnull(request_model) \
    | eval idx = (timestampdiff(MICROSECOND, `timestamp`, timestamp('2023-11-14 23:13:20')) - 1) / 60000000, dur_ms = duration_micros / 1000.0D \
    | stats percentile(dur_ms, 50) as p0 by idx, request_model";

/// `token_usage_sql(window, 60 s)`.
const TOKEN_SERIES_SQL: &str = r#"SELECT "request_model", arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)') AS "bucket", cast(sum(total_tokens) as double) AS "tokens"
FROM "iceberg"."icegate"."operations"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000)) AND (request_model is not null)
GROUP BY "request_model", arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)')
ORDER BY arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)') ASC"#;

const TOKEN_SERIES_PPL: &str = "source=operations \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') and isnotnull(request_model) \
    | eval idx = (timestampdiff(MICROSECOND, `timestamp`, timestamp('2023-11-14 23:13:20')) - 1) / 60000000 \
    | stats sum(total_tokens) as tokens by idx, request_model \
    | eval tokens = cast(tokens as double)";

/// `bucket_scalars_sql(window, 60 s)`.
const BUCKET_SCALARS_SQL: &str = r#"SELECT arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)') AS "bucket", count(*) AS "events"
FROM "iceberg"."icegate"."operations"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000))
GROUP BY arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)')
ORDER BY arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)') ASC"#;

const BUCKET_SCALARS_PPL: &str = "source=operations \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | eval idx = (timestampdiff(MICROSECOND, `timestamp`, timestamp('2023-11-14 23:13:20')) - 1) / 60000000 \
    | stats count() as events by idx";

/// `bucket_cost_sql(window, 60 s)`.
const BUCKET_COST_SQL: &str = r#"WITH priced AS (SELECT o.*, p.input_usd_per_1m, p.output_usd_per_1m, p.cache_read_usd_per_1m, p.cache_write_usd_per_1m, p.request_usd, row_number() OVER (PARTITION BY o.trace_id, o.span_id ORDER BY p.valid_from DESC, p.min_input_tokens DESC) AS rn
  FROM (SELECT * FROM iceberg.icegate.operations WHERE timestamp >= to_timestamp_micros(1700000000000000) AND timestamp < to_timestamp_micros(1700003600000000)) o
  LEFT JOIN iceberg.icegate.prices_effective p ON p.provider = o.provider_name AND p.model = o.request_model AND p.service_tier = 'standard' AND p.region = 'global' AND (p.min_input_tokens IS NULL OR coalesce(o.input_tokens, 0) >= p.min_input_tokens) AND (p.max_input_tokens IS NULL OR coalesce(o.input_tokens, 0) < p.max_input_tokens) AND o.timestamp >= p.valid_from AND (p.valid_to IS NULL OR o.timestamp < p.valid_to))
SELECT arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)') AS "bucket", coalesce(sum(coalesce(o.input_tokens, 0) / 1e6 * o.input_usd_per_1m + coalesce(o.output_tokens, 0) / 1e6 * o.output_usd_per_1m + coalesce(o.cache_read_input_tokens, 0) / 1e6 * coalesce(o.cache_read_usd_per_1m, 0) + coalesce(o.cache_creation_input_tokens, 0) / 1e6 * coalesce(o.cache_write_usd_per_1m, 0) + coalesce(o.request_usd, 0)), 0) AS "cost_usd"
FROM "priced" AS "o"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000)) AND (o.rn = 1)
GROUP BY arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)')
ORDER BY arrow_cast(date_bin(interval '60 seconds', timestamp, to_timestamp_micros(1700003600000000)), 'Timestamp(Microsecond, None)') ASC"#;

const BUCKET_COST_PPL: &str = "source=operations \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | left join max=0 left=o right=p on o.provider_name = p.provider and o.request_model = p.model and (isnull(p.min_input_tokens) or coalesce(o.input_tokens, 0) >= p.min_input_tokens) and (isnull(p.max_input_tokens) or coalesce(o.input_tokens, 0) < p.max_input_tokens) and o.`timestamp` >= p.valid_from and (isnull(p.valid_to) or o.`timestamp` < p.valid_to) \
        [ source=prices_effective | where service_tier = 'standard' and region = 'global' | fields provider, model, min_input_tokens, max_input_tokens, valid_from, valid_to, input_usd_per_1m, output_usd_per_1m, cache_read_usd_per_1m, cache_write_usd_per_1m, request_usd ] \
    | sort - valid_from, - min_input_tokens \
    | dedup trace_id, span_id \
    | eval idx = (timestampdiff(MICROSECOND, `timestamp`, timestamp('2023-11-14 23:13:20')) - 1) / 60000000, cost = coalesce(input_tokens, 0) / 1000000.0D * input_usd_per_1m + coalesce(output_tokens, 0) / 1000000.0D * output_usd_per_1m + coalesce(cache_read_input_tokens, 0) / 1000000.0D * coalesce(cache_read_usd_per_1m, 0) + coalesce(cache_creation_input_tokens, 0) / 1000000.0D * coalesce(cache_write_usd_per_1m, 0) + coalesce(request_usd, 0) \
    | stats sum(cost) as cost_usd by idx \
    | eval cost_usd = coalesce(cost_usd, 0.0D)";

/// `llm_durations_sql(window)`.
const LLM_DURATIONS_SQL: &str = r#"SELECT cast(duration_micros as double) / 1000.0 AS "ms" FROM "iceberg"."icegate"."operations" WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000)) AND (operation_name in ('chat','text_completion','generate_content')) LIMIT 5000"#;

const LLM_DURATIONS_PPL: &str = "source=operations \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') and operation_name in ('chat', 'text_completion', 'generate_content') \
    | eval ms = cast(duration_micros as double) / 1000.0D \
    | fields ms \
    | head 5000";

/// `text` rendered as the canonical timestamp form, back to microseconds since the epoch.
///
/// # Errors
///
/// Returns the parse failure when `text` is not in the canonical form.
fn parse_rendered_timestamp(text: &str) -> Result<i64, String> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .map(|timestamp| timestamp.and_utc().timestamp_micros())
        .map_err(|error| format!("`{text}` is not a rendered timestamp: {error}"))
}

/// SQL rows with their `bucket` timestamp replaced by `bucket_us`, its microseconds.
///
/// # Errors
///
/// Returns which row is not an object, lacks `bucket`, or holds a bucket that is not a rendered
/// timestamp.
fn rebucket_sql_rows(rows: Vec<Value>) -> Result<Vec<Value>, String> {
    rows.into_iter()
        .map(|mut row| -> Result<Value, String> {
            let object = row.as_object_mut().ok_or("a SQL row is not an object")?;
            let bucket = object.remove("bucket").ok_or("a SQL row has no `bucket` column")?;
            let rendered_bucket = bucket.as_str().ok_or_else(|| format!("SQL bucket {bucket} is not a string"))?;
            let bucket_us = parse_rendered_timestamp(rendered_bucket)?;
            object.insert("bucket_us".to_owned(), json!(bucket_us));
            Ok(row)
        })
        .collect()
}

/// PPL rows with their bucket index `idx` replaced by `bucket_us = window_end - (idx + 1) * step`.
///
/// # Errors
///
/// Returns which row is not an object, lacks `idx`, or holds an `idx` that is not an integer.
fn rebucket_ppl_rows(rows: Vec<Value>) -> Result<Vec<Value>, String> {
    rows.into_iter()
        .map(|mut row| -> Result<Value, String> {
            let object = row.as_object_mut().ok_or("a PPL row is not an object")?;
            let index_cell = object.remove("idx").ok_or("a PPL row has no `idx` column")?;
            let bucket_index = index_cell
                .as_i64()
                .ok_or_else(|| format!("PPL idx {index_cell} is not an integer"))?;
            object.insert(
                "bucket_us".to_owned(),
                json!(WINDOW_END_MICROS - (bucket_index + 1) * STEP_MICROS),
            );
            Ok(row)
        })
        .collect()
}

/// One expected latency row: its kind, then each percentile column with the expected value
/// (`None` expects null) and the tolerance it is compared within.
struct KindLatencyRow {
    kind: &'static str,
    cells: [(&'static str, Option<f64>, f64); 4],
}

/// Assert one row per expected kind, each percentile within its tolerance.
fn assert_latency_rows(form: &str, actual: &[Value], expected: &[KindLatencyRow]) {
    assert_eq!(actual.len(), expected.len(), "{form}: {actual:#?}");
    for expected_row in expected {
        let row = actual
            .iter()
            .find(|row| row["kind"] == expected_row.kind)
            .unwrap_or_else(|| panic!("{form}: no `{}` row in {actual:#?}", expected_row.kind));
        for (column, value, tolerance) in expected_row.cells {
            match value {
                None => assert!(
                    row[column].is_null(),
                    "{form} {}: {column} is {}",
                    expected_row.kind,
                    row[column]
                ),
                Some(value) => {
                    let cell = row[column]
                        .as_f64()
                        .unwrap_or_else(|| panic!("{form} {}: {column} is {}", expected_row.kind, row[column]));
                    assert!(
                        (cell - value).abs() <= tolerance,
                        "{form} {}: {column} is {cell}, expected {value} within {tolerance}",
                        expected_row.kind
                    );
                }
            }
        }
    }
}

/// In the window, A has five operations, three with status 2: the one exactly at the inclusive
/// start counts. A's errored operations one second before the start, exactly at the exclusive end
/// and after it are out, and so is B's.
#[tokio::test]
async fn kpi_counts_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_overview(&catalog).await;
    let expected = [json!({"events": 5, "errored": 3})];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, COUNTS_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, COUNTS_PPL).await, &expected);
    servers.shutdown().await;
}

/// The 101 LLM durations are 1000, 1010, …, 2000 ms: the 0.5 quantile is 1500 and the 0.95
/// quantile 1950 (rank `q * 100` over the sorted values), the first chunks 150 and 195. Both
/// forms estimate, so the LLM cells compare within 20 ms and 2 ms; a single-value kind is exact.
/// A kind without first chunks has null first-chunk percentiles. B's `reranker` call would add a
/// fifth row, of kind `retrieval`, if it leaked.
#[tokio::test]
async fn latency_percentiles_per_kind_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    commit_operation_lines(&catalog, &build_latency_lines()).await;
    let expected = [
        KindLatencyRow {
            kind: "llm",
            cells: [
                ("dur_p0", Some(1500.0), 20.0),
                ("ttft_p0", Some(150.0), 2.0),
                ("dur_p1", Some(1950.0), 20.0),
                ("ttft_p1", Some(195.0), 2.0),
            ],
        },
        KindLatencyRow {
            kind: "tool",
            cells: [
                ("dur_p0", Some(200.0), 0.001),
                ("ttft_p0", None, 0.0),
                ("dur_p1", Some(200.0), 0.001),
                ("ttft_p1", None, 0.0),
            ],
        },
        KindLatencyRow {
            kind: "embeddings",
            cells: [
                ("dur_p0", Some(50.0), 0.001),
                ("ttft_p0", None, 0.0),
                ("dur_p1", Some(50.0), 0.001),
                ("ttft_p1", None, 0.0),
            ],
        },
        KindLatencyRow {
            kind: "agent",
            cells: [
                ("dur_p0", Some(400.0), 0.001),
                ("ttft_p0", Some(40.0), 0.001),
                ("dur_p1", Some(400.0), 0.001),
                ("ttft_p1", Some(40.0), 0.001),
            ],
        },
    ];
    assert_latency_rows("SQL", &servers.run_sql(TENANT_A, KIND_LATENCY_SQL).await, &expected);
    assert_latency_rows("PPL", &servers.run_ppl(TENANT_A, KIND_LATENCY_PPL).await, &expected);
    servers.shutdown().await;
}

/// `0.006 + 0.012` USD from the two `gpt-4o` calls at the current revision; the tool call and the
/// chat at the window start, which names no model, add nothing and are not unpriced;
/// `text-embed` has no rate and is the one unpriced operation. The million-token `gpt-4o` calls
/// just outside both window edges would add dollars if they leaked.
#[tokio::test]
async fn window_cost_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_overview(&catalog).await;
    let expected = [json!({"cost_usd": 0.018, "unpriced_operations": 1})];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, WINDOW_COST_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, WINDOW_COST_PPL).await, &expected);
    servers.shutdown().await;
}

/// Buckets anchored at 23:13:20 with a 60 s step start at 22:13:20 + 60 k s, so the operations
/// at +0 s and +30 s fall into the bucket starting at +0 s, and those at +90 s, +150 s and
/// +210 s into the buckets starting at +60, +120 and +180 s. The operation at +0 s names no
/// model, so it shows only in the event count. Single-value percentiles are exact.
#[tokio::test]
async fn anchored_time_buckets_match_their_sql_forms() {
    let (servers, catalog) = ParityServers::start().await;
    seed_overview(&catalog).await;
    let latency = [
        json!({"request_model": "gpt-4o", "bucket_us": 1_700_000_000_000_000_i64, "p0": 1000.0}),
        json!({"request_model": "gpt-4o", "bucket_us": 1_700_000_060_000_000_i64, "p0": 3000.0}),
        json!({"request_model": "text-embed", "bucket_us": 1_700_000_180_000_000_i64, "p0": 50.0}),
    ];
    let tokens = [
        json!({"request_model": "gpt-4o", "bucket_us": 1_700_000_000_000_000_i64, "tokens": 1500.0}),
        json!({"request_model": "gpt-4o", "bucket_us": 1_700_000_060_000_000_i64, "tokens": 3000.0}),
        json!({"request_model": "text-embed", "bucket_us": 1_700_000_180_000_000_i64, "tokens": 10.0}),
    ];
    let scalars = [
        json!({"bucket_us": 1_700_000_000_000_000_i64, "events": 2}),
        json!({"bucket_us": 1_700_000_060_000_000_i64, "events": 1}),
        json!({"bucket_us": 1_700_000_120_000_000_i64, "events": 1}),
        json!({"bucket_us": 1_700_000_180_000_000_i64, "events": 1}),
    ];
    let costs = [
        json!({"bucket_us": 1_700_000_000_000_000_i64, "cost_usd": 0.006}),
        json!({"bucket_us": 1_700_000_060_000_000_i64, "cost_usd": 0.012}),
        json!({"bucket_us": 1_700_000_120_000_000_i64, "cost_usd": 0.0}),
        json!({"bucket_us": 1_700_000_180_000_000_i64, "cost_usd": 0.0}),
    ];
    let cases: [(&str, &str, &str, &[Value]); 4] = [
        ("latency timeseries", LATENCY_SERIES_SQL, LATENCY_SERIES_PPL, &latency),
        ("token usage", TOKEN_SERIES_SQL, TOKEN_SERIES_PPL, &tokens),
        ("bucket scalars", BUCKET_SCALARS_SQL, BUCKET_SCALARS_PPL, &scalars),
        ("bucket cost", BUCKET_COST_SQL, BUCKET_COST_PPL, &costs),
    ];
    for (case, sql, ppl, expected) in cases {
        let sql_rows = rebucket_sql_rows(servers.run_sql(TENANT_A, sql).await).expect("SQL buckets");
        assert_rows_unordered(&format!("SQL {case}"), &sql_rows, expected);
        let ppl_rows = rebucket_ppl_rows(servers.run_ppl(TENANT_A, ppl).await).expect("PPL buckets");
        assert_rows_unordered(&format!("PPL {case}"), &ppl_rows, expected);
    }
    servers.shutdown().await;
}

/// The three `chat` durations of A in the window, the one at its inclusive start among them; B's
/// 99 s chat and A's chats one second before the window, exactly at its exclusive end and after
/// it stay out. The statement carries no order.
#[tokio::test]
async fn llm_durations_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_overview(&catalog).await;
    let expected = [json!({"ms": 2500.0}), json!({"ms": 1000.0}), json!({"ms": 3000.0})];
    assert_rows_unordered("SQL", &servers.run_sql(TENANT_A, LLM_DURATIONS_SQL).await, &expected);
    assert_rows_unordered("PPL", &servers.run_ppl(TENANT_A, LLM_DURATIONS_PPL).await, &expected);
    servers.shutdown().await;
}
