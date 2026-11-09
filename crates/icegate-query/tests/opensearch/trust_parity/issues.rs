//! trust's issue-trace list over `marks`: the traces holding the live marks of one Issue, newest
//! last sighting first, then trace id ascending, with a keyset continuation in `HAVING`.
//!
//! Rendered for the L0 Issue `fp-one` over the day `[2023-11-14 22:13:20, 2023-11-15 22:13:20)`,
//! three traces per page.
#![allow(clippy::expect_used)]

use serde_json::{Value, json};

use super::harness::{ParityServers, SECOND_MICROS, TENANT_A, TENANT_B, WINDOW_START_MICROS, assert_rows_in_order};
use super::tables::{MarkRow, commit_mark_rows, repeat_fingerprint};

/// `fp-one` marks of A on traces `01…` (two marks, +100 s low priced `0.5` and +500 s high),
/// `02…` (+500 s medium `0.25`), `03…` (+300 s low, unpriced) and `04…` (+50 s low `1.0`); an
/// `fp-other` mark of A at +600 s; and B's `fp-one` mark on trace `09…` at +1000 s, which would
/// lead the list if it leaked.
fn build_issue_trace_rows() -> Vec<MarkRow> {
    let window_instant = |offset_seconds: i64| WINDOW_START_MICROS + offset_seconds * SECOND_MICROS;
    let build_first_issue_mark = |tenant: &'static str, number: u64, offset_seconds: i64, trace_byte: u8| {
        MarkRow::new(
            tenant,
            number,
            window_instant(offset_seconds),
            [trace_byte; 16],
            repeat_fingerprint("fp-one"),
        )
    };
    vec![
        MarkRow {
            impact_cost_usd: Some("0.5"),
            ..build_first_issue_mark(TENANT_A, 0x401, 100, 0x01)
        },
        MarkRow {
            severity: "high",
            ..build_first_issue_mark(TENANT_A, 0x402, 500, 0x01)
        },
        MarkRow {
            severity: "medium",
            impact_cost_usd: Some("0.25"),
            ..build_first_issue_mark(TENANT_A, 0x403, 500, 0x02)
        },
        build_first_issue_mark(TENANT_A, 0x404, 300, 0x03),
        MarkRow {
            impact_cost_usd: Some("1.0"),
            ..build_first_issue_mark(TENANT_A, 0x405, 50, 0x04)
        },
        MarkRow::new(
            TENANT_A,
            0x406,
            window_instant(600),
            [0x05; 16],
            repeat_fingerprint("fp-other"),
        ),
        MarkRow {
            impact_cost_usd: Some("3.0"),
            ..build_first_issue_mark(TENANT_B, 0x4b1, 1_000, 0x09)
        },
    ]
}

/// One expected trace row; `priced_cost` is the priced-mark count and the cost sum, `severities`
/// the low, medium and high counts.
fn expect_trace_row(
    trace_hex: &str,
    last_us: i64,
    marks: u64,
    priced_cost: (u64, Option<f64>),
    severities: [u64; 3],
) -> Value {
    let (priced, cost_sum) = priced_cost;
    json!({"trace_id": trace_hex, "last_us": last_us, "marks": marks, "priced": priced, "cost_sum": cost_sum,
        "sev_low": severities[0], "sev_medium": severities[1], "sev_high": severities[2]})
}

/// `issue_traces_sql` for the first page (`after = None`, `limit = 3`).
const FIRST_PAGE_SQL: &str = r"WITH ranked AS (
  SELECT m.*, row_number() OVER (PARTITION BY m.mark_id ORDER BY m.scanned_at DESC, m.scan_id DESC, m.attempt DESC) AS rn
  FROM iceberg.icegate.marks m
  WHERE m.occurred_at >= to_timestamp_micros(1700000000000000) AND m.occurred_at < to_timestamp_micros(1700086400000000) AND (true)
),
live AS (SELECT * FROM ranked WHERE rn = 1 AND NOT retracted),
held AS (
  SELECT live.*, 0 AS issue_level
  FROM live WHERE (fingerprint = 'fp-one')
)
SELECT trace_id,
  arrow_cast(max(occurred_at), 'Int64') AS last_us,
  count(*) AS marks,
  count(impact_cost_usd) AS priced,
  CAST(sum(impact_cost_usd) AS DOUBLE) AS cost_sum,
  count(CASE WHEN severity = 'low' THEN 1 END) AS sev_low,
  count(CASE WHEN severity = 'medium' THEN 1 END) AS sev_medium,
  count(CASE WHEN severity = 'high' THEN 1 END) AS sev_high
FROM held
WHERE (CASE WHEN issue_level = 3 AND localization_kind IN ('code', 'node') THEN 2 ELSE issue_level END) = 0
GROUP BY trace_id
ORDER BY last_us DESC, trace_id ASC LIMIT 3";

/// The keyset page after `(+500 s, 0101…01)`.
const KEYSET_PAGE_SQL: &str = r"WITH ranked AS (
  SELECT m.*, row_number() OVER (PARTITION BY m.mark_id ORDER BY m.scanned_at DESC, m.scan_id DESC, m.attempt DESC) AS rn
  FROM iceberg.icegate.marks m
  WHERE m.occurred_at >= to_timestamp_micros(1700000000000000) AND m.occurred_at < to_timestamp_micros(1700086400000000) AND (true)
),
live AS (SELECT * FROM ranked WHERE rn = 1 AND NOT retracted),
held AS (
  SELECT live.*, 0 AS issue_level
  FROM live WHERE (fingerprint = 'fp-one')
)
SELECT trace_id,
  arrow_cast(max(occurred_at), 'Int64') AS last_us,
  count(*) AS marks,
  count(impact_cost_usd) AS priced,
  CAST(sum(impact_cost_usd) AS DOUBLE) AS cost_sum,
  count(CASE WHEN severity = 'low' THEN 1 END) AS sev_low,
  count(CASE WHEN severity = 'medium' THEN 1 END) AS sev_medium,
  count(CASE WHEN severity = 'high' THEN 1 END) AS sev_high
FROM held
WHERE (CASE WHEN issue_level = 3 AND localization_kind IN ('code', 'node') THEN 2 ELSE issue_level END) = 0
GROUP BY trace_id
HAVING max(occurred_at) < to_timestamp_micros(1700000500000000) OR (max(occurred_at) = to_timestamp_micros(1700000500000000) AND trace_id > arrow_cast(decode('01010101010101010101010101010101', 'hex'), 'FixedSizeBinary(16)'))
ORDER BY last_us DESC, trace_id ASC LIMIT 3";

const FIRST_PAGE_PPL: &str = "source=marks \
    | where occurred_at >= timestamp('2023-11-14 22:13:20') and occurred_at < timestamp('2023-11-15 22:13:20') \
    | sort - scanned_at, - scan_id, - attempt \
    | dedup mark_id \
    | where not retracted \
    | eval issue_level = 0 \
    | where fingerprint = 'fp-one' \
    | where case(issue_level = 3 and localization_kind in ('code', 'node'), 2 else issue_level) = 0 \
    | stats max(occurred_at) as last_seen, count() as marks, count(impact_cost_usd) as priced, sum(impact_cost_usd) as cost_sum, count(eval(severity = 'low')) as sev_low, count(eval(severity = 'medium')) as sev_medium, count(eval(severity = 'high')) as sev_high by trace_id \
    | eval last_us = timestampdiff(MICROSECOND, timestamp('1970-01-01 00:00:00'), last_seen) \
    | sort - last_us, trace_id \
    | head 3 \
    | fields trace_id, last_us, marks, priced, cost_sum, sev_low, sev_medium, sev_high";

const KEYSET_PAGE_PPL: &str = "source=marks \
    | where occurred_at >= timestamp('2023-11-14 22:13:20') and occurred_at < timestamp('2023-11-15 22:13:20') \
    | sort - scanned_at, - scan_id, - attempt \
    | dedup mark_id \
    | where not retracted \
    | eval issue_level = 0 \
    | where fingerprint = 'fp-one' \
    | where case(issue_level = 3 and localization_kind in ('code', 'node'), 2 else issue_level) = 0 \
    | stats max(occurred_at) as last_seen, count() as marks, count(impact_cost_usd) as priced, sum(impact_cost_usd) as cost_sum, count(eval(severity = 'low')) as sev_low, count(eval(severity = 'medium')) as sev_medium, count(eval(severity = 'high')) as sev_high by trace_id \
    | where last_seen < timestamp('2023-11-14 22:21:40') or (last_seen = timestamp('2023-11-14 22:21:40') and trace_id > '01010101010101010101010101010101') \
    | eval last_us = timestampdiff(MICROSECOND, timestamp('1970-01-01 00:00:00'), last_seen) \
    | sort - last_us, trace_id \
    | head 3 \
    | fields trace_id, last_us, marks, priced, cost_sum, sev_low, sev_medium, sev_high";

/// Traces `01…` and `02…` share their last sighting (+500 s) and order by id; the first page
/// stops after `03…`, the keyset page starts after `01…`.
#[tokio::test]
async fn issue_traces_first_page_and_keyset_page_match_their_sql_forms() {
    let (servers, catalog) = ParityServers::start().await;
    commit_mark_rows(&catalog, &build_issue_trace_rows()).await;
    let first_trace_row = expect_trace_row(
        "01010101010101010101010101010101",
        1_700_000_500_000_000,
        2,
        (1, Some(0.5)),
        [1, 0, 1],
    );
    let second_trace_row = expect_trace_row(
        "02020202020202020202020202020202",
        1_700_000_500_000_000,
        1,
        (1, Some(0.25)),
        [0, 1, 0],
    );
    let third_trace_row = expect_trace_row(
        "03030303030303030303030303030303",
        1_700_000_300_000_000,
        1,
        (0, None),
        [1, 0, 0],
    );
    let fourth_trace_row = expect_trace_row(
        "04040404040404040404040404040404",
        1_700_000_050_000_000,
        1,
        (1, Some(1.0)),
        [1, 0, 0],
    );
    let first_page = [first_trace_row, second_trace_row.clone(), third_trace_row.clone()];
    let keyset_page = [second_trace_row, third_trace_row, fourth_trace_row];
    assert_rows_in_order(
        "SQL first page",
        &servers.run_sql(TENANT_A, FIRST_PAGE_SQL).await,
        &first_page,
    );
    assert_rows_in_order(
        "PPL first page",
        &servers.run_ppl(TENANT_A, FIRST_PAGE_PPL).await,
        &first_page,
    );
    assert_rows_in_order(
        "SQL keyset page",
        &servers.run_sql(TENANT_A, KEYSET_PAGE_SQL).await,
        &keyset_page,
    );
    assert_rows_in_order(
        "PPL keyset page",
        &servers.run_ppl(TENANT_A, KEYSET_PAGE_PPL).await,
        &keyset_page,
    );
    servers.shutdown().await;
}
