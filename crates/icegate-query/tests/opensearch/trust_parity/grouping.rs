//! trust's two grouping statements over `marks`: the Issue grain past its cap of 20 000
//! (`LIMIT 20001`), and the fine grain, an unbounded read with sparkline buckets.
//!
//! Both are rendered unfiltered, unrestricted and with no raised generation, over the day
//! `[2023-11-14 22:13:20, 2023-11-15 22:13:20)`.
#![allow(clippy::expect_used)]

use serde_json::{Value, json};

use super::harness::{
    ParityServers, SECOND_MICROS, TENANT_A, TENANT_B, WINDOW_START_MICROS, assert_rows_in_order, assert_rows_unordered,
};
use super::tables::{MarkRow, MarkScope, commit_mark_rows, format_mark_id, repeat_fingerprint};

/// Single-mark Issues of A: more than the Issue grain returns. Issue numbers are `u16`, so every
/// conversion from an Issue number to a mark number, trace id or offset is lossless.
const SINGLE_ISSUES: u16 = 20_004;
/// Rows the Issue grain returns: its cap plus the one that shows the cap bound.
const ISSUE_GRAIN_ROWS: usize = 20_001;
/// Single-mark Issues of A added to the fine-grain seed to carry its answer past 10 000 cells.
const FINE_GRAIN_SINGLE_ISSUES: u16 = 10_001;
/// The first mark number of those Issues, above every mark number the fine-grain seed states,
/// since the live view dedups by `mark_id`.
const FINE_GRAIN_MARK_BASE: u64 = 0x1_0000;

/// The fingerprint of single Issue `number`: 32 lowercase hex digits.
fn format_single_fingerprint(number: u16) -> String {
    format!("{number:032x}")
}

/// When single Issue `number`'s mark occurred: within the first hour of the day.
fn compute_single_occurred_at(number: u16) -> i64 {
    WINDOW_START_MICROS + i64::from(number % 3_600) * SECOND_MICROS
}

/// The one mark of single Issue `number`, on its own trace.
fn build_single_mark(number: u16) -> MarkRow {
    let trace_id = (u128::from(number) + 1).to_be_bytes();
    MarkRow::new(
        TENANT_A,
        u64::from(number),
        compute_single_occurred_at(number),
        trace_id,
        repeat_fingerprint(&format_single_fingerprint(number)),
    )
}

/// The Issue-grain seed of A: 20 004 single-mark Issues; Issue 1's mark retracted by a later
/// scan; Issue 2's mark first scanned under another fingerprint and then superseded; Issue
/// `zz-big-1` with three marks on three traces whose newest carries the anchor `new`; Issue
/// `zz-big-2` with two marks on two traces. Tenant B adds five marks on five traces under Issue
/// 0's fingerprint, which would rank Issue 0 first if they leaked.
///
/// Four more `zz-big-2` marks, on traces of their own, are retracted by a row scanned at the same
/// instant as their live row: the retractions of 0xff06 and 0xff08 win on `scan_id` alone, those
/// of 0xff07 and 0xff09 on `attempt` alone. Each tie is seeded in both batch orders (the live row
/// of 0xff06 and 0xff07 precedes its retraction, that of 0xff08 and 0xff09 follows it), so a live
/// view ranked by `scanned_at` alone, which leaves tied rows in batch order, keeps a live row of a
/// tie and adds its trace to `zz-big-2` whether it keeps the first or the last tied row.
fn build_issue_grain_rows() -> Vec<MarkRow> {
    let mut rows: Vec<MarkRow> = (0..SINGLE_ISSUES).map(build_single_mark).collect();
    let retracted_mark = build_single_mark(1);
    rows.push(retracted_mark.build_retraction(retracted_mark.occurred_at_micros + SECOND_MICROS));
    let superseded_mark = build_single_mark(2);
    rows.push(MarkRow {
        scan_id: "scan-0",
        scanned_at_micros: superseded_mark.occurred_at_micros - SECOND_MICROS,
        fingerprints: repeat_fingerprint("000000000000000000000000000000-x"),
        ..superseded_mark
    });
    for (number, offset_seconds, trace_byte, anchor) in [
        (0xff01, 10, 0xf1, "old"),
        (0xff02, 20, 0xf2, "old"),
        (0xff03, 30, 0xf3, "new"),
    ] {
        rows.push(MarkRow {
            anchor_name: Some(anchor),
            ..MarkRow::new(
                TENANT_A,
                number,
                WINDOW_START_MICROS + offset_seconds * SECOND_MICROS,
                [trace_byte; 16],
                repeat_fingerprint("zz-big-1"),
            )
        });
    }
    let build_second_big_issue_mark = |number: u64, offset_seconds: i64, trace_byte: u8| {
        MarkRow::new(
            TENANT_A,
            number,
            WINDOW_START_MICROS + offset_seconds * SECOND_MICROS,
            [trace_byte; 16],
            repeat_fingerprint("zz-big-2"),
        )
    };
    for (number, offset_seconds, trace_byte) in [(0xff04, 40, 0xf4), (0xff05, 50, 0xf5)] {
        rows.push(build_second_big_issue_mark(number, offset_seconds, trace_byte));
    }
    let scan_tied_mark = build_second_big_issue_mark(0xff06, 60, 0xf6);
    let attempt_tied_mark = build_second_big_issue_mark(0xff07, 70, 0xf7);
    let late_scan_tied_mark = build_second_big_issue_mark(0xff08, 80, 0xf8);
    let late_attempt_tied_mark = build_second_big_issue_mark(0xff09, 90, 0xf9);
    rows.extend([
        scan_tied_mark.clone(),
        MarkRow {
            scan_id: "scan-2",
            is_retracted: true,
            ..scan_tied_mark
        },
        attempt_tied_mark.clone(),
        MarkRow {
            attempt: 2,
            is_retracted: true,
            ..attempt_tied_mark
        },
        MarkRow {
            scan_id: "scan-2",
            is_retracted: true,
            ..late_scan_tied_mark.clone()
        },
        late_scan_tied_mark,
        MarkRow {
            attempt: 2,
            is_retracted: true,
            ..late_attempt_tied_mark.clone()
        },
        late_attempt_tied_mark,
    ]);
    for (number, trace_byte) in (0xb001_u64..).zip(0xb1_u8..=0xb5) {
        rows.push(MarkRow::new(
            TENANT_B,
            number,
            WINDOW_START_MICROS,
            [trace_byte; 16],
            repeat_fingerprint(&format_single_fingerprint(0)),
        ));
    }
    rows
}

/// An expected Issue-grain row with the defaults of [`MarkRow::new`].
fn expect_issue_row(fingerprint: &str, distinct_traces: u64, last_live_us: i64, anchor_name: &str) -> Value {
    json!({
        "issue_fp": fingerprint, "issue_level": 0, "rule_id": "rule-a", "fp_version": 1, "fp_epoch": 1,
        "localization_kind": "code", "localization_key": "src/lib.rs:10", "distinct_traces": distinct_traces,
        "last_live_us": last_live_us, "t_anchor_name": anchor_name, "t_variant": null, "t_evidence": "{}",
        "t_service_name": "svc", "t_environment": null, "t_agent_name": null, "t_agent_version": null,
        "t_workflow_name": null, "t_model": null, "t_provider": null
    })
}

/// `issue_rows_sql` (the `unfiltered` id).
const ISSUE_ROWS_SQL: &str = r"WITH ranked AS (
  SELECT m.*, row_number() OVER (PARTITION BY m.mark_id ORDER BY m.scanned_at DESC, m.scan_id DESC, m.attempt DESC) AS rn
  FROM iceberg.icegate.marks m
  WHERE m.occurred_at >= to_timestamp_micros(1700000000000000) AND m.occurred_at < to_timestamp_micros(1700086400000000) AND (true)
),
live AS (SELECT * FROM ranked WHERE rn = 1 AND NOT retracted),
keyed AS (
  SELECT live.*, fingerprint AS issue_fp, 0 AS issue_level, true AS matches
  FROM live WHERE true
),
picked AS (
  SELECT keyed.*, row_number() OVER (PARTITION BY issue_fp, issue_level, matches ORDER BY occurred_at DESC, mark_id DESC) AS newest
  FROM keyed
)
SELECT issue_fp, issue_level, min(rule_id) AS rule_id, min(fp_version) AS fp_version, min(fp_epoch) AS fp_epoch,
  min(localization_kind) AS localization_kind, min(localization_key) AS localization_key, count(DISTINCT CASE WHEN matches THEN trace_id END) AS distinct_traces,
  arrow_cast(max(occurred_at), 'Int64') AS last_live_us,
  max(CASE WHEN matches AND newest = 1 THEN anchor_name END) AS t_anchor_name,
  max(CASE WHEN matches AND newest = 1 THEN variant END) AS t_variant,
  max(CASE WHEN matches AND newest = 1 THEN evidence END) AS t_evidence,
  max(CASE WHEN matches AND newest = 1 THEN service_name END) AS t_service_name,
  max(CASE WHEN matches AND newest = 1 THEN environment END) AS t_environment,
  max(CASE WHEN matches AND newest = 1 THEN agent_name END) AS t_agent_name,
  max(CASE WHEN matches AND newest = 1 THEN agent_version END) AS t_agent_version,
  max(CASE WHEN matches AND newest = 1 THEN workflow_name END) AS t_workflow_name,
  max(CASE WHEN matches AND newest = 1 THEN model END) AS t_model,
  max(CASE WHEN matches AND newest = 1 THEN provider END) AS t_provider
FROM picked GROUP BY issue_fp, issue_level HAVING count(CASE WHEN matches THEN 1 END) > 0 ORDER BY distinct_traces DESC, issue_level, issue_fp LIMIT 20001";

const ISSUE_ROWS_PPL: &str = "source=marks \
    | where occurred_at >= timestamp('2023-11-14 22:13:20') and occurred_at < timestamp('2023-11-15 22:13:20') \
    | sort - scanned_at, - scan_id, - attempt \
    | dedup mark_id \
    | where not retracted \
    | eval issue_fp = fingerprint, issue_level = 0, matches = true \
    | sort - occurred_at, - mark_id \
    | streamstats count() as newest by issue_fp, issue_level, matches \
    | eval matched_trace = case(matches, trace_id), is_title = matches and newest = 1, title_anchor = case(is_title, anchor_name), title_variant = case(is_title, variant), title_evidence = case(is_title, evidence), title_service = case(is_title, service_name), title_environment = case(is_title, environment), title_agent = case(is_title, agent_name), title_agent_version = case(is_title, agent_version), title_workflow = case(is_title, workflow_name), title_model = case(is_title, model), title_provider = case(is_title, provider) \
    | stats min(rule_id) as rule_id, min(fp_version) as fp_version, min(fp_epoch) as fp_epoch, min(localization_kind) as localization_kind, min(localization_key) as localization_key, dc(matched_trace) as distinct_traces, max(occurred_at) as last_live, max(title_anchor) as t_anchor_name, max(title_variant) as t_variant, max(title_evidence) as t_evidence, max(title_service) as t_service_name, max(title_environment) as t_environment, max(title_agent) as t_agent_name, max(title_agent_version) as t_agent_version, max(title_workflow) as t_workflow_name, max(title_model) as t_model, max(title_provider) as t_provider, count(eval(matches)) as matched by issue_fp, issue_level \
    | where matched > 0 \
    | eval last_live_us = timestampdiff(MICROSECOND, timestamp('1970-01-01 00:00:00'), last_live) \
    | sort - distinct_traces, issue_level, issue_fp \
    | head 20001 \
    | fields issue_fp, issue_level, rule_id, fp_version, fp_epoch, localization_kind, localization_key, distinct_traces, last_live_us, t_anchor_name, t_variant, t_evidence, t_service_name, t_environment, t_agent_name, t_agent_version, t_workflow_name, t_model, t_provider";

/// The fine-grain seed: Issue `fp-one` with two marks of cell `(planner, flow, 1.0)` (one in
/// each sparkline bucket) and one of `(planner, flow, 2.0)`; Issue `fp-two` with one mark without
/// agent or workflow; a retracted `fp-one` mark; and B's `fp-one` mark of cell `1.0`.
///
/// Four more `fp-two` marks are retracted by a row scanned at the same instant as their live
/// row: the retractions of 0x106 and 0x108 win on `scan_id` alone, those of 0x107 and 0x109 on
/// `attempt` alone. Each tie is seeded in both batch orders (the live row of 0x106 and 0x107
/// precedes its retraction, that of 0x108 and 0x109 follows it), so a live view ranked by
/// `scanned_at` alone, which leaves tied rows in batch order, keeps a live row of a tie and adds
/// it to the `fp-two` cell whether it keeps the first or the last tied row. A sort that reorders
/// equal keys might keep only retractions; the test's control run would then fail rather than
/// pass silently.
fn build_fine_grain_rows() -> Vec<MarkRow> {
    let planner_scope = |version: &'static str| MarkScope {
        service_name: Some("svc"),
        agent_name: Some("planner"),
        workflow_name: Some("flow"),
        agent_version: Some(version),
        ..MarkScope::default()
    };
    let window_instant = |offset_seconds: i64| WINDOW_START_MICROS + offset_seconds * SECOND_MICROS;
    let retracted_mark = MarkRow {
        scope: planner_scope("1.0"),
        impact_cost_usd: Some("9.0"),
        ..MarkRow::new(
            TENANT_A,
            0x105,
            window_instant(400),
            [0x65; 16],
            repeat_fingerprint("fp-one"),
        )
    };
    let scan_tied_mark = MarkRow {
        scanned_at_micros: window_instant(501),
        ..MarkRow::new(
            TENANT_A,
            0x106,
            window_instant(500),
            [0x67; 16],
            repeat_fingerprint("fp-two"),
        )
    };
    let attempt_tied_mark = MarkRow {
        scanned_at_micros: window_instant(601),
        ..MarkRow::new(
            TENANT_A,
            0x107,
            window_instant(600),
            [0x68; 16],
            repeat_fingerprint("fp-two"),
        )
    };
    let late_scan_tied_mark = MarkRow {
        scanned_at_micros: window_instant(701),
        ..MarkRow::new(
            TENANT_A,
            0x108,
            window_instant(700),
            [0x69; 16],
            repeat_fingerprint("fp-two"),
        )
    };
    let late_attempt_tied_mark = MarkRow {
        scanned_at_micros: window_instant(801),
        ..MarkRow::new(
            TENANT_A,
            0x109,
            window_instant(800),
            [0x6a; 16],
            repeat_fingerprint("fp-two"),
        )
    };
    vec![
        MarkRow {
            scope: planner_scope("1.0"),
            root_span_id: Some([0x71; 8]),
            impact_cost_usd: Some("0.5"),
            impact_added_latency_micros: Some(1000),
            impact_input_tokens: Some(100),
            impact_output_tokens: Some(10),
            impact_cacheable_tokens: Some(5),
            run_quality_z: Some(0.1),
            ..MarkRow::new(
                TENANT_A,
                0x101,
                window_instant(100),
                [0x61; 16],
                repeat_fingerprint("fp-one"),
            )
        },
        MarkRow {
            scope: planner_scope("1.0"),
            severity: "high",
            impact_cost_usd: Some("0.25"),
            impact_input_tokens: Some(200),
            ..MarkRow::new(
                TENANT_A,
                0x102,
                window_instant(50_000),
                [0x62; 16],
                repeat_fingerprint("fp-one"),
            )
        },
        MarkRow {
            scope: planner_scope("2.0"),
            severity: "medium",
            ..MarkRow::new(
                TENANT_A,
                0x103,
                window_instant(200),
                [0x63; 16],
                repeat_fingerprint("fp-one"),
            )
        },
        MarkRow {
            impact_cost_usd: Some("1.0"),
            ..MarkRow::new(
                TENANT_A,
                0x104,
                window_instant(300),
                [0x64; 16],
                repeat_fingerprint("fp-two"),
            )
        },
        retracted_mark.build_retraction(window_instant(401)),
        retracted_mark,
        scan_tied_mark.clone(),
        MarkRow {
            scan_id: "scan-2",
            is_retracted: true,
            ..scan_tied_mark
        },
        attempt_tied_mark.clone(),
        MarkRow {
            attempt: 2,
            is_retracted: true,
            ..attempt_tied_mark
        },
        MarkRow {
            scan_id: "scan-2",
            is_retracted: true,
            ..late_scan_tied_mark.clone()
        },
        late_scan_tied_mark,
        MarkRow {
            attempt: 2,
            is_retracted: true,
            ..late_attempt_tied_mark.clone()
        },
        late_attempt_tied_mark,
        MarkRow {
            scope: planner_scope("1.0"),
            severity: "high",
            impact_cost_usd: Some("7.0"),
            ..MarkRow::new(
                TENANT_B,
                0x1b1,
                window_instant(150),
                [0x66; 16],
                repeat_fingerprint("fp-one"),
            )
        },
    ]
}

/// `fine_rows_sql` with sparkline edges `[22:13:20, 2023-11-15 10:13:20, 2023-11-15 22:13:20]`.
const FINE_ROWS_SQL: &str = r"WITH ranked AS (
  SELECT m.*, row_number() OVER (PARTITION BY m.mark_id ORDER BY m.scanned_at DESC, m.scan_id DESC, m.attempt DESC) AS rn
  FROM iceberg.icegate.marks m
  WHERE m.occurred_at >= to_timestamp_micros(1700000000000000) AND m.occurred_at < to_timestamp_micros(1700086400000000) AND (true)
),
live AS (SELECT * FROM ranked WHERE rn = 1 AND NOT retracted),
keyed AS (
  SELECT live.*, fingerprint AS issue_fp, 0 AS issue_level,
    concat(coalesce(agent_name, '-'), '|', coalesce(workflow_name, '-')) AS class_key
  FROM live WHERE true AND true
),
kept AS (
  SELECT issue_fp, issue_level, count(DISTINCT trace_id) AS distinct_traces
  FROM keyed GROUP BY issue_fp, issue_level ORDER BY distinct_traces DESC, issue_level, issue_fp LIMIT 20000
),
ends AS (
  SELECT keyed.*,
    first_value(mark_id) OVER (PARTITION BY issue_fp, issue_level, fingerprint, class_key, agent_name, agent_version ORDER BY occurred_at, mark_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS cell_first_mark,
    last_value(mark_id) OVER (PARTITION BY issue_fp, issue_level, fingerprint, class_key, agent_name, agent_version ORDER BY occurred_at, mark_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS cell_last_mark
  FROM keyed JOIN (SELECT issue_fp AS kept_fp, issue_level AS kept_level FROM kept) AS k ON keyed.issue_fp = k.kept_fp AND keyed.issue_level = k.kept_level
)
SELECT issue_fp, issue_level, fingerprint AS l0, class_key, agent_name, agent_version,
  count(*) AS marks,
  arrow_cast(min(occurred_at), 'Int64') AS first_us,
  arrow_cast(max(occurred_at), 'Int64') AS last_us,
  min(cell_first_mark) AS first_mark_id,
  min(cell_last_mark) AS last_mark_id,
  count(impact_cost_usd) AS priced,
  CAST(sum(impact_cost_usd) AS DOUBLE) AS cost_sum,
  count(impact_added_latency_micros) AS latency_measured,
  count(root_span_id) AS quality_eligible,
  count(CASE WHEN root_span_id IS NOT NULL THEN run_quality_z END) AS quality_compared,
  sum(impact_input_tokens) AS input_tokens,
  sum(impact_output_tokens) AS output_tokens,
  sum(impact_cacheable_tokens) AS cacheable_tokens,
  sum(impact_added_latency_micros) AS added_latency_micros,
  count(CASE WHEN severity = 'low' THEN 1 END) AS sev_low,
  count(CASE WHEN severity = 'medium' THEN 1 END) AS sev_medium,
  count(CASE WHEN severity = 'high' THEN 1 END) AS sev_high,
  count(CASE WHEN occurred_at >= to_timestamp_micros(1700000000000000) AND occurred_at < to_timestamp_micros(1700043200000000) THEN 1 END) AS bucket_0,
  count(CASE WHEN occurred_at >= to_timestamp_micros(1700043200000000) AND occurred_at < to_timestamp_micros(1700086400000000) THEN 1 END) AS bucket_1
FROM ends GROUP BY issue_fp, issue_level, fingerprint, class_key, agent_name, agent_version";

const FINE_ROWS_PPL: &str = "source=marks \
    | where occurred_at >= timestamp('2023-11-14 22:13:20') and occurred_at < timestamp('2023-11-15 22:13:20') \
    | sort - scanned_at, - scan_id, - attempt \
    | dedup mark_id \
    | where not retracted \
    | eval issue_fp = fingerprint, issue_level = 0, class_key = concat(coalesce(agent_name, '-'), '|', coalesce(workflow_name, '-')) \
    | inner join max=0 left=c right=k on c.issue_fp = k.kept_fp and c.issue_level = k.kept_level \
        [ source=marks | where occurred_at >= timestamp('2023-11-14 22:13:20') and occurred_at < timestamp('2023-11-15 22:13:20') | sort - scanned_at, - scan_id, - attempt | dedup mark_id | where not retracted | eval kept_fp = fingerprint, kept_level = 0 | stats dc(trace_id) as distinct_traces by kept_fp, kept_level | sort - distinct_traces, kept_level, kept_fp | head 20000 | fields kept_fp, kept_level ] \
    | eventstats bucket_nullable=true min(occurred_at) as cell_first_at, max(occurred_at) as cell_last_at by issue_fp, issue_level, fingerprint, class_key, agent_name, agent_version \
    | eval first_candidate = case(occurred_at = cell_first_at, mark_id), last_candidate = case(occurred_at = cell_last_at, mark_id) \
    | stats bucket_nullable=true count() as marks, min(occurred_at) as first_at, max(occurred_at) as last_at, min(first_candidate) as first_mark_id, max(last_candidate) as last_mark_id, count(impact_cost_usd) as priced, sum(impact_cost_usd) as cost_sum, count(impact_added_latency_micros) as latency_measured, count(root_span_id) as quality_eligible, count(eval(isnotnull(root_span_id) and isnotnull(run_quality_z))) as quality_compared, sum(impact_input_tokens) as input_tokens, sum(impact_output_tokens) as output_tokens, sum(impact_cacheable_tokens) as cacheable_tokens, sum(impact_added_latency_micros) as added_latency_micros, count(eval(severity = 'low')) as sev_low, count(eval(severity = 'medium')) as sev_medium, count(eval(severity = 'high')) as sev_high, count(eval(occurred_at >= timestamp('2023-11-14 22:13:20') and occurred_at < timestamp('2023-11-15 10:13:20'))) as bucket_0, count(eval(occurred_at >= timestamp('2023-11-15 10:13:20') and occurred_at < timestamp('2023-11-15 22:13:20'))) as bucket_1 by issue_fp, issue_level, fingerprint, class_key, agent_name, agent_version \
    | eval first_us = timestampdiff(MICROSECOND, timestamp('1970-01-01 00:00:00'), first_at), last_us = timestampdiff(MICROSECOND, timestamp('1970-01-01 00:00:00'), last_at) \
    | rename fingerprint as l0 \
    | fields issue_fp, issue_level, l0, class_key, agent_name, agent_version, marks, first_us, last_us, first_mark_id, last_mark_id, priced, cost_sum, latency_measured, quality_eligible, quality_compared, input_tokens, output_tokens, cacheable_tokens, added_latency_micros, sev_low, sev_medium, sev_high, bucket_0, bucket_1";

/// 20 005 live Issues of A; the grain keeps `zz-big-1` (3 traces, titled by its newest mark),
/// `zz-big-2` (2 traces: its four tied marks 0xff06 to 0xff09 are retracted), then single Issues
/// by fingerprint: 0, then 2 to 19 999 (Issue 1 is retracted, Issue 2 is live under its newest
/// fingerprint only). B's five marks under Issue 0's fingerprint would make Issue 0 lead with six
/// traces.
///
/// A control run of the same PPL form ranking the live view by `scanned_at` alone must count more
/// than two `zz-big-2` traces: that proves the seed reaches the tie the full ranking key breaks.
#[tokio::test]
async fn issue_rows_over_twenty_thousand_rows_match_with_head_20001() {
    let (servers, catalog) = ParityServers::start().await;
    commit_mark_rows(&catalog, &build_issue_grain_rows()).await;
    let mut expected = vec![
        expect_issue_row("zz-big-1", 3, WINDOW_START_MICROS + 30 * SECOND_MICROS, "new"),
        expect_issue_row("zz-big-2", 2, WINDOW_START_MICROS + 50 * SECOND_MICROS, "anchor"),
    ];
    expected.extend(
        std::iter::once(0)
            .chain(2..SINGLE_ISSUES)
            .take(ISSUE_GRAIN_ROWS - 2)
            .map(|number| {
                expect_issue_row(
                    &format_single_fingerprint(number),
                    1,
                    compute_single_occurred_at(number),
                    "anchor",
                )
            }),
    );
    assert_eq!(expected.len(), ISSUE_GRAIN_ROWS);
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, ISSUE_ROWS_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, ISSUE_ROWS_PPL).await, &expected);
    assert_eq!(
        ISSUE_ROWS_PPL.matches("sort - scanned_at, - scan_id, - attempt").count(),
        1,
        "the control must rank the live view by `scanned_at` alone"
    );
    let scanned_at_ranking_ppl = ISSUE_ROWS_PPL.replace("sort - scanned_at, - scan_id, - attempt", "sort - scanned_at");
    let control_rows = servers.run_ppl(TENANT_A, &scanned_at_ranking_ppl).await;
    let second_big_issue_row = control_rows
        .iter()
        .find(|row| row["issue_fp"] == "zz-big-2")
        .unwrap_or_else(|| panic!("control: no `zz-big-2` row among {} rows", control_rows.len()));
    let second_big_issue_trace_count = second_big_issue_row["distinct_traces"].as_u64().unwrap_or_else(|| {
        panic!(
            "control: `zz-big-2` distinct_traces is {}",
            second_big_issue_row["distinct_traces"]
        )
    });
    assert!(
        second_big_issue_trace_count > 2,
        "control: a live view ranked by `scanned_at` alone counted {second_big_issue_trace_count} `zz-big-2` trace(s)"
    );
    servers.shutdown().await;
}

/// The three cells of [`build_fine_grain_rows`].
fn expect_fine_literal_cells() -> [Value; 3] {
    [
        json!({"issue_fp": "fp-one", "issue_level": 0, "l0": "fp-one", "class_key": "planner|flow", "agent_name": "planner", "agent_version": "1.0",
            "marks": 2, "first_us": 1_700_000_100_000_000_i64, "last_us": 1_700_050_000_000_000_i64,
            "first_mark_id": format_mark_id(0x101), "last_mark_id": format_mark_id(0x102), "priced": 2, "cost_sum": 0.75,
            "latency_measured": 1, "quality_eligible": 1, "quality_compared": 1, "input_tokens": 300, "output_tokens": 10,
            "cacheable_tokens": 5, "added_latency_micros": 1000, "sev_low": 1, "sev_medium": 0, "sev_high": 1, "bucket_0": 1, "bucket_1": 1}),
        json!({"issue_fp": "fp-one", "issue_level": 0, "l0": "fp-one", "class_key": "planner|flow", "agent_name": "planner", "agent_version": "2.0",
            "marks": 1, "first_us": 1_700_000_200_000_000_i64, "last_us": 1_700_000_200_000_000_i64,
            "first_mark_id": format_mark_id(0x103), "last_mark_id": format_mark_id(0x103), "priced": 0, "cost_sum": null,
            "latency_measured": 0, "quality_eligible": 0, "quality_compared": 0, "input_tokens": null, "output_tokens": null,
            "cacheable_tokens": null, "added_latency_micros": null, "sev_low": 0, "sev_medium": 1, "sev_high": 0, "bucket_0": 1, "bucket_1": 0}),
        json!({"issue_fp": "fp-two", "issue_level": 0, "l0": "fp-two", "class_key": "-|-", "agent_name": null, "agent_version": null,
            "marks": 1, "first_us": 1_700_000_300_000_000_i64, "last_us": 1_700_000_300_000_000_i64,
            "first_mark_id": format_mark_id(0x104), "last_mark_id": format_mark_id(0x104), "priced": 1, "cost_sum": 1.0,
            "latency_measured": 0, "quality_eligible": 0, "quality_compared": 0, "input_tokens": null, "output_tokens": null,
            "cacheable_tokens": null, "added_latency_micros": null, "sev_low": 1, "sev_medium": 0, "sev_high": 0, "bucket_0": 1, "bucket_1": 0}),
    ]
}

/// The one cell of the single-mark Issue `number` the over-ten-thousand case adds: the defaults
/// of [`MarkRow::new`], one low-severity mark in the first sparkline bucket, no impact figures.
fn expect_single_fine_cell(number: u16) -> Value {
    let fingerprint = format_single_fingerprint(number);
    let occurred_at = compute_single_occurred_at(number);
    let mark_id = format_mark_id(FINE_GRAIN_MARK_BASE + u64::from(number));
    json!({"issue_fp": fingerprint, "issue_level": 0, "l0": fingerprint, "class_key": "-|-", "agent_name": null, "agent_version": null,
        "marks": 1, "first_us": occurred_at, "last_us": occurred_at,
        "first_mark_id": mark_id, "last_mark_id": mark_id, "priced": 0, "cost_sum": null,
        "latency_measured": 0, "quality_eligible": 0, "quality_compared": 0, "input_tokens": null, "output_tokens": null,
        "cacheable_tokens": null, "added_latency_micros": null, "sev_low": 1, "sev_medium": 0, "sev_high": 0, "bucket_0": 1, "bucket_1": 0})
}

/// Three cells. `(fp-one, planner|flow, 1.0)` holds marks 0x101 (+100 s, bucket 0) and 0x102
/// (+50 000 s, bucket 1): two priced (`0.5 + 0.25`), one with latency, root span and quality
/// score, tokens `100 + 200` / `10` / `5`. `(fp-one, planner|flow, 2.0)` holds 0x103 alone,
/// unpriced. `(fp-two, -|-, null, null)` holds 0x104 at `1.0`. The retracted 0x105 to 0x109 and
/// B's 0x1b1 are not counted.
///
/// A control run of the same PPL form ranking both live views (the read and the kept-Issue
/// subsearch) by `scanned_at` alone must count more than one `fp-two` mark: that proves the seed
/// reaches the tie the full ranking key breaks.
#[tokio::test]
async fn fine_rows_unbounded_read_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    commit_mark_rows(&catalog, &build_fine_grain_rows()).await;
    let expected = expect_fine_literal_cells();
    assert_rows_unordered("SQL", &servers.run_sql(TENANT_A, FINE_ROWS_SQL).await, &expected);
    assert_rows_unordered("PPL", &servers.run_ppl(TENANT_A, FINE_ROWS_PPL).await, &expected);
    let scanned_at_ranking_ppl = FINE_ROWS_PPL.replace("sort - scanned_at, - scan_id, - attempt", "sort - scanned_at");
    assert_eq!(
        FINE_ROWS_PPL.matches("sort - scanned_at, - scan_id, - attempt").count(),
        2,
        "the control must rank both live views by `scanned_at` alone"
    );
    let control_rows = servers.run_ppl(TENANT_A, &scanned_at_ranking_ppl).await;
    let second_issue_cell = control_rows
        .iter()
        .find(|row| row["issue_fp"] == "fp-two")
        .unwrap_or_else(|| panic!("control: no `fp-two` cell in {control_rows:#?}"));
    let second_issue_mark_count = second_issue_cell["marks"]
        .as_u64()
        .unwrap_or_else(|| panic!("control: `fp-two` marks is {}", second_issue_cell["marks"]));
    assert!(
        second_issue_mark_count > 1,
        "control: a live view ranked by `scanned_at` alone counted {second_issue_mark_count} `fp-two` mark(s)"
    );
    servers.shutdown().await;
}

/// The unbounded read past ten thousand rows: the fine-grain seed plus 10 001 single-mark Issues
/// of A return 10 004 cells. The outer `stats` has no `head`, so a PPL answer shorter than
/// 10 004 rows is a cap on the read or on its `stats` groups, which the never-capped rule
/// forbids; the kept subsearch's `head 20000` lies above the 10 003 Issues it ranks.
#[tokio::test]
async fn fine_rows_over_ten_thousand_cells_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    let mut rows = build_fine_grain_rows();
    rows.extend((0..FINE_GRAIN_SINGLE_ISSUES).map(|number| {
        let mark_number = FINE_GRAIN_MARK_BASE + u64::from(number);
        MarkRow::new(
            TENANT_A,
            mark_number,
            compute_single_occurred_at(number),
            u128::from(mark_number).to_be_bytes(),
            repeat_fingerprint(&format_single_fingerprint(number)),
        )
    }));
    commit_mark_rows(&catalog, &rows).await;
    let mut expected: Vec<Value> = expect_fine_literal_cells().into();
    expected.extend((0..FINE_GRAIN_SINGLE_ISSUES).map(expect_single_fine_cell));
    assert!(expected.len() > 10_000);
    assert_eq!(expected.len(), 10_004);
    assert_rows_unordered("SQL", &servers.run_sql(TENANT_A, FINE_ROWS_SQL).await, &expected);
    assert_rows_unordered("PPL", &servers.run_ppl(TENANT_A, FINE_ROWS_PPL).await, &expected);
    servers.shutdown().await;
}
