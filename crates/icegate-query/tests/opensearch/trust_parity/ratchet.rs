//! trust's ratchet statements over `marks`: distinct fingerprints per generation, UTC day and
//! level, and the regroup mapping between two levels of one generation, both over the period
//! `[2023-11-14 00:00:00, 2023-11-16 00:00:00)`.
#![allow(clippy::expect_used)]

use serde_json::json;

use super::harness::{ParityServers, SECOND_MICROS, TENANT_A, TENANT_B, assert_rows_unordered};
use super::tables::{MarkRow, commit_mark_rows};

/// `2023-11-14 00:00:00` UTC: the first day of the period.
const DAY_ONE_MICROS: i64 = 1_699_920_000_000_000;
/// One day in microseconds.
const DAY_MICROS: i64 = 86_400 * SECOND_MICROS;

/// The four level fingerprints of a ratchet mark.
fn build_level_fingerprints(fingerprints: [&str; 4]) -> [String; 4] {
    fingerprints.map(str::to_owned)
}

/// Day one: `r1` v1 marks `(a, x, p, q)`, `(b, x, p, q)`, `(b, y, p, q)`. Day two: `r1` v1 marks
/// `(a, x, p, q)` and `(d, w, u, v)` (localized `node`), `r2` v2 `(c, z, s, t)`, `r1` v2
/// `(e, e1, u, e3)`, and an `r1` v1 mark `(zz, zz1, zz2, zz3)` a later scan retracts. B adds an
/// `r1` v1 mark `(c9, c9, c9, c9)` on day one. Every mark is epoch 1 on trace `70…`.
fn build_ratchet_rows() -> Vec<MarkRow> {
    let hour = 3_600 * SECOND_MICROS;
    let day_two = DAY_ONE_MICROS + DAY_MICROS;
    let trace = [0x70; 16];
    let build_rule_r1_mark = |number: u64, occurred_at_micros: i64, fingerprints: [&str; 4]| MarkRow {
        rule_id: "r1",
        ..MarkRow::new(
            TENANT_A,
            number,
            occurred_at_micros,
            trace,
            build_level_fingerprints(fingerprints),
        )
    };
    let retracted_mark = build_rule_r1_mark(0x308, day_two + 3 * hour, ["zz", "zz1", "zz2", "zz3"]);
    vec![
        build_rule_r1_mark(0x301, DAY_ONE_MICROS + hour, ["a", "x", "p", "q"]),
        build_rule_r1_mark(0x302, DAY_ONE_MICROS + 2 * hour, ["b", "x", "p", "q"]),
        build_rule_r1_mark(0x303, DAY_ONE_MICROS + 3 * hour, ["b", "y", "p", "q"]),
        build_rule_r1_mark(0x304, day_two + hour, ["a", "x", "p", "q"]),
        MarkRow {
            localization_kind: "node",
            ..build_rule_r1_mark(0x305, day_two + 2 * hour, ["d", "w", "u", "v"])
        },
        MarkRow {
            rule_id: "r2",
            fp_version: 2,
            ..MarkRow::new(
                TENANT_A,
                0x306,
                day_two + hour,
                trace,
                build_level_fingerprints(["c", "z", "s", "t"]),
            )
        },
        MarkRow {
            fp_version: 2,
            ..build_rule_r1_mark(0x307, day_two + hour, ["e", "e1", "u", "e3"])
        },
        retracted_mark.build_retraction(day_two + 4 * hour),
        retracted_mark,
        MarkRow {
            rule_id: "r1",
            ..MarkRow::new(
                TENANT_B,
                0x3b1,
                DAY_ONE_MICROS + hour,
                trace,
                build_level_fingerprints(["c9", "c9", "c9", "c9"]),
            )
        },
    ]
}

/// `day_counts_sql(period)`.
const DAY_COUNTS_SQL: &str = r"WITH ranked AS (
  SELECT m.*, row_number() OVER (PARTITION BY m.mark_id ORDER BY m.scanned_at DESC, m.scan_id DESC, m.attempt DESC) AS rn
  FROM iceberg.icegate.marks m
  WHERE m.occurred_at >= to_timestamp_micros(1699920000000000) AND m.occurred_at < to_timestamp_micros(1700092800000000) AND (true)
),
live AS (SELECT * FROM ranked WHERE rn = 1 AND NOT retracted)
SELECT rule_id, fp_version, fp_epoch,
  count(DISTINCT fingerprint) AS c0, count(DISTINCT fingerprint_l1) AS c1,
  count(DISTINCT fingerprint_l2) AS c2, count(DISTINCT fingerprint_l3) AS c3
FROM live GROUP BY rule_id, fp_version, fp_epoch, date_trunc('day', occurred_at)";

const DAY_COUNTS_PPL: &str = "source=marks \
    | where occurred_at >= timestamp('2023-11-14 00:00:00') and occurred_at < timestamp('2023-11-16 00:00:00') \
    | sort - scanned_at, - scan_id, - attempt \
    | dedup mark_id \
    | where not retracted \
    | stats dc(fingerprint) as c0, dc(fingerprint_l1) as c1, dc(fingerprint_l2) as c2, dc(fingerprint_l3) as c3 by span(occurred_at, 1d) as day, rule_id, fp_version, fp_epoch \
    | fields rule_id, fp_version, fp_epoch, c0, c1, c2, c3";

/// `regroup_pairs_sql(period, (r1, 1, 1), L0, L2)`.
const REGROUP_PAIRS_SQL: &str = r"WITH ranked AS (
  SELECT m.*, row_number() OVER (PARTITION BY m.mark_id ORDER BY m.scanned_at DESC, m.scan_id DESC, m.attempt DESC) AS rn
  FROM iceberg.icegate.marks m
  WHERE m.occurred_at >= to_timestamp_micros(1699920000000000) AND m.occurred_at < to_timestamp_micros(1700092800000000) AND (rule_id = 'r1')
),
live AS (SELECT * FROM ranked WHERE rn = 1 AND NOT retracted)
SELECT fingerprint_l2 AS new_fp, min(fingerprint) AS old_fp, count(DISTINCT fingerprint) AS n, min(localization_kind) AS kind
FROM live WHERE fp_version = 1 AND fp_epoch = 1 GROUP BY fingerprint_l2";

const REGROUP_PAIRS_PPL: &str = "source=marks \
    | where occurred_at >= timestamp('2023-11-14 00:00:00') and occurred_at < timestamp('2023-11-16 00:00:00') and rule_id = 'r1' \
    | sort - scanned_at, - scan_id, - attempt \
    | dedup mark_id \
    | where not retracted \
    | where fp_version = 1 and fp_epoch = 1 \
    | stats min(fingerprint) as old_fp, dc(fingerprint) as n, min(localization_kind) as kind by fingerprint_l2 \
    | rename fingerprint_l2 as new_fp \
    | fields new_fp, old_fp, n, kind";

/// Per generation and day: `r1` v1 day one has fingerprints `a, b`, level-1 `x, y`, level-2 `p`,
/// level-3 `q`; `r1` v1 day two `a, d` / `x, w` / `p, u` / `q, v` (the retracted mark is not
/// live); `r2` v2 and `r1` v2 one of each on day two. B's mark would add `c9` to day one.
#[tokio::test]
async fn level_budget_day_counts_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    commit_mark_rows(&catalog, &build_ratchet_rows()).await;
    let expected = [
        json!({"rule_id": "r1", "fp_version": 1, "fp_epoch": 1, "c0": 2, "c1": 2, "c2": 1, "c3": 1}),
        json!({"rule_id": "r1", "fp_version": 1, "fp_epoch": 1, "c0": 2, "c1": 2, "c2": 2, "c3": 2}),
        json!({"rule_id": "r2", "fp_version": 2, "fp_epoch": 1, "c0": 1, "c1": 1, "c2": 1, "c3": 1}),
        json!({"rule_id": "r1", "fp_version": 2, "fp_epoch": 1, "c0": 1, "c1": 1, "c2": 1, "c3": 1}),
    ];
    assert_rows_unordered("SQL", &servers.run_sql(TENANT_A, DAY_COUNTS_SQL).await, &expected);
    assert_rows_unordered("PPL", &servers.run_ppl(TENANT_A, DAY_COUNTS_PPL).await, &expected);
    servers.shutdown().await;
}

/// Generation `(r1, 1, 1)` from L0 to L2: level-2 `p` holds `a` and `b` (two, so not one-to-one);
/// `u` holds `d` alone (the `r1` v2 mark under `u` belongs to another generation).
#[tokio::test]
async fn regroup_pairs_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    commit_mark_rows(&catalog, &build_ratchet_rows()).await;
    let expected = [
        json!({"new_fp": "p", "old_fp": "a", "n": 2, "kind": "code"}),
        json!({"new_fp": "u", "old_fp": "d", "n": 1, "kind": "node"}),
    ];
    assert_rows_unordered("SQL", &servers.run_sql(TENANT_A, REGROUP_PAIRS_SQL).await, &expected);
    assert_rows_unordered("PPL", &servers.run_ppl(TENANT_A, REGROUP_PAIRS_PPL).await, &expected);
    servers.shutdown().await;
}
