//! trust's harm statement over `verdicts`: every harm row at its `(subject, class)` pair's
//! newest `computed_at` within the three hours before the pinned end, both bounds inclusive.
#![allow(clippy::expect_used)]

use serde_json::json;

use super::harness::{ParityServers, SECOND_MICROS, TENANT_A, TENANT_B, WINDOW_START_MICROS, assert_rows_unordered};
use super::tables::VERDICTS_TABLE;
use crate::opensearch::data::{Cell, Row, commit_rows};

/// One `verdicts` row; it was computed `offset_seconds` after the window start.
struct VerdictLine {
    tenant: &'static str,
    verdict_kind: &'static str,
    subject_key: &'static str,
    class_key: &'static str,
    pillar: &'static str,
    p_adj: f64,
    effect: Option<f64>,
    effect_ci_low: Option<f64>,
    effect_ci_high: Option<f64>,
    n_subject: i64,
    holds: bool,
    harm: Option<&'static str>,
    offset_seconds: i64,
}

/// The pinned end is the window start plus three hours, so the bound is `[+0 s, +10 800 s]`.
/// Pair `aaaa…` has an older and a newer A row; `bbbb…` one row; `cccc…` is not a harm row;
/// `dddd…` falls one second outside each bound; `eeee…` sits on the inclusive end with a `NaN`
/// effect; `ffff…` sits on the inclusive start. B's row for pair `aaaa…` is newer than A's.
static VERDICT_LINES: [VerdictLine; 9] = [
    VerdictLine {
        tenant: TENANT_A,
        verdict_kind: "harm",
        subject_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        class_key: "agent|flow",
        pillar: "cost",
        p_adj: 0.0004,
        effect: Some(0.02),
        effect_ci_low: Some(0.01),
        effect_ci_high: Some(0.03),
        n_subject: 40,
        holds: true,
        harm: Some("harmful"),
        offset_seconds: 3_600,
    },
    VerdictLine {
        tenant: TENANT_A,
        verdict_kind: "harm",
        subject_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        class_key: "agent|flow",
        pillar: "cost",
        p_adj: 0.9,
        effect: None,
        effect_ci_low: None,
        effect_ci_high: None,
        n_subject: 5,
        holds: false,
        harm: None,
        offset_seconds: 600,
    },
    VerdictLine {
        tenant: TENANT_A,
        verdict_kind: "harm",
        subject_key: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        class_key: "-|-",
        pillar: "failure",
        p_adj: 1.0,
        effect: None,
        effect_ci_low: None,
        effect_ci_high: None,
        n_subject: 12,
        holds: false,
        harm: None,
        offset_seconds: 1_200,
    },
    VerdictLine {
        tenant: TENANT_A,
        verdict_kind: "quality",
        subject_key: "cccccccccccccccccccccccccccccccc",
        class_key: "agent|flow",
        pillar: "quality",
        p_adj: 0.01,
        effect: Some(0.5),
        effect_ci_low: None,
        effect_ci_high: None,
        n_subject: 10,
        holds: true,
        harm: None,
        offset_seconds: 1_800,
    },
    VerdictLine {
        tenant: TENANT_A,
        verdict_kind: "harm",
        subject_key: "dddddddddddddddddddddddddddddddd",
        class_key: "-|-",
        pillar: "cost",
        p_adj: 0.2,
        effect: None,
        effect_ci_low: None,
        effect_ci_high: None,
        n_subject: 8,
        holds: false,
        harm: None,
        offset_seconds: -1,
    },
    VerdictLine {
        tenant: TENANT_A,
        verdict_kind: "harm",
        subject_key: "dddddddddddddddddddddddddddddddd",
        class_key: "-|-",
        pillar: "cost",
        p_adj: 0.3,
        effect: None,
        effect_ci_low: None,
        effect_ci_high: None,
        n_subject: 9,
        holds: false,
        harm: None,
        offset_seconds: 10_801,
    },
    VerdictLine {
        tenant: TENANT_A,
        verdict_kind: "harm",
        subject_key: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        class_key: "agent|flow",
        pillar: "latency",
        p_adj: 0.5,
        effect: Some(f64::NAN),
        effect_ci_low: None,
        effect_ci_high: None,
        n_subject: 20,
        holds: false,
        harm: None,
        offset_seconds: 10_800,
    },
    VerdictLine {
        tenant: TENANT_A,
        verdict_kind: "harm",
        subject_key: "ffffffffffffffffffffffffffffffff",
        class_key: "-|-",
        pillar: "latency",
        p_adj: 0.05,
        effect: Some(0.25),
        effect_ci_low: None,
        effect_ci_high: None,
        n_subject: 7,
        holds: true,
        harm: Some("harmful"),
        offset_seconds: 0,
    },
    VerdictLine {
        tenant: TENANT_B,
        verdict_kind: "harm",
        subject_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        class_key: "agent|flow",
        pillar: "cost",
        p_adj: 0.7,
        effect: None,
        effect_ci_low: None,
        effect_ci_high: None,
        n_subject: 3,
        holds: false,
        harm: None,
        offset_seconds: 7_200,
    },
];

/// One line as a `verdicts` row.
fn build_verdict_row(line: &'static VerdictLine) -> Row<'static> {
    let mut cells = vec![
        ("verdict_kind", Cell::Text(line.verdict_kind)),
        ("subject_key", Cell::Text(line.subject_key)),
        ("class_key", Cell::Text(line.class_key)),
        ("pillar", Cell::Text(line.pillar)),
        ("p_adj", Cell::Double(line.p_adj)),
        ("n_subject", Cell::Long(line.n_subject)),
        ("holds", Cell::Bool(line.holds)),
        (
            "computed_at",
            Cell::Micros(WINDOW_START_MICROS + line.offset_seconds * SECOND_MICROS),
        ),
    ];
    let statistics = [
        ("effect", line.effect),
        ("effect_ci_low", line.effect_ci_low),
        ("effect_ci_high", line.effect_ci_high),
    ];
    cells.extend(
        statistics
            .into_iter()
            .filter_map(|(column, value)| value.map(|number| (column, Cell::Double(number)))),
    );
    if let Some(harm) = line.harm {
        cells.push(("harm", Cell::Text(harm)));
    }
    Row {
        tenant: line.tenant,
        cells,
    }
}

/// `harm_rows_sql(pinned_end = 2023-11-15 01:13:20)`.
const HARM_ROWS_SQL: &str = r"WITH v AS (
  SELECT subject_key, class_key, pillar, p_adj, effect, effect_ci_low, effect_ci_high, n_subject, holds, harm, computed_at,
    max(computed_at) OVER (PARTITION BY subject_key, class_key) AS newest
  FROM iceberg.icegate.verdicts
  WHERE verdict_kind = 'harm' AND computed_at >= to_timestamp_micros(1700000000000000) AND computed_at <= to_timestamp_micros(1700010800000000)
)
SELECT subject_key, class_key, pillar, p_adj, effect, effect_ci_low, effect_ci_high, n_subject, holds, harm
FROM v WHERE computed_at = newest";

const HARM_ROWS_PPL: &str = "source=verdicts \
    | where verdict_kind = 'harm' and computed_at >= timestamp('2023-11-14 22:13:20') and computed_at <= timestamp('2023-11-15 01:13:20') \
    | eventstats max(computed_at) as newest by subject_key, class_key \
    | where computed_at = newest \
    | fields subject_key, class_key, pillar, p_adj, effect, effect_ci_low, effect_ci_high, n_subject, holds, harm";

/// A's newest harm row of each pair inside the bound, both ends included (`eeee…` at the end,
/// `ffff…` at the start); the `NaN` effect is the string `NaN`, never null. B's newer row for
/// pair `aaaa…` would drop A's if it leaked into the window function.
#[tokio::test]
async fn harm_rows_match_their_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    let rows: Vec<Row<'static>> = VERDICT_LINES.iter().map(build_verdict_row).collect();
    commit_rows(&catalog, VERDICTS_TABLE, &rows).await.expect("seed verdicts");
    let expected = [
        json!({"subject_key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "class_key": "agent|flow", "pillar": "cost", "p_adj": 0.0004,
            "effect": 0.02, "effect_ci_low": 0.01, "effect_ci_high": 0.03, "n_subject": 40, "holds": true, "harm": "harmful"}),
        json!({"subject_key": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "class_key": "-|-", "pillar": "failure", "p_adj": 1.0,
            "effect": null, "effect_ci_low": null, "effect_ci_high": null, "n_subject": 12, "holds": false, "harm": null}),
        json!({"subject_key": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "class_key": "agent|flow", "pillar": "latency", "p_adj": 0.5,
            "effect": "NaN", "effect_ci_low": null, "effect_ci_high": null, "n_subject": 20, "holds": false, "harm": null}),
        json!({"subject_key": "ffffffffffffffffffffffffffffffff", "class_key": "-|-", "pillar": "latency", "p_adj": 0.05,
            "effect": 0.25, "effect_ci_low": null, "effect_ci_high": null, "n_subject": 7, "holds": true, "harm": "harmful"}),
    ];
    assert_rows_unordered("SQL", &servers.run_sql(TENANT_A, HARM_ROWS_SQL).await, &expected);
    assert_rows_unordered("PPL", &servers.run_ppl(TENANT_A, HARM_ROWS_PPL).await, &expected);
    servers.shutdown().await;
}
