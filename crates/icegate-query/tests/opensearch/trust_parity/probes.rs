//! trust's test probes: the readiness probe, the row-count poll, and the two rate-card probes.
#![allow(clippy::expect_used)]

use icegate_common::SPANS_TABLE;
use icegate_common::schema::{
    COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_NAME, COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID,
};
use serde_json::{Value, json};

use super::harness::{ParityServers, TENANT_A, TENANT_B, WINDOW_START_MICROS, assert_rows_in_order};
use super::tables::commit_rate_card;
use crate::opensearch::data::{Cell, Row, commit_rows};

/// `query_until_ready`'s statement.
const READINESS_SQL: &str = "SELECT 1 AS one";
/// The PPL readiness probe: one row even when the table is empty.
const READINESS_PPL: &str = "source=spans | stats count() as n";

/// `await_rows_at_least(…, "spans", …)`.
const SPANS_COUNT_SQL: &str = "SELECT count(*) AS n FROM iceberg.icegate.spans";
const SPANS_COUNT_PPL: &str = "source=spans | stats count() as n";
/// `await_rows_at_least(…, "scans", …)` over trust's empty audit table.
const SCANS_COUNT_SQL: &str = "SELECT count(*) AS n FROM iceberg.icegate.scans";
const SCANS_COUNT_PPL: &str = "source=scans | stats count() as n";

/// `require_prices_effective`'s probe.
const RATE_CARD_PROBE_SQL: &str = "SELECT 1 FROM iceberg.icegate.prices_effective LIMIT 1";
/// The PPL probe answers a literal, so its one row is asserted by value.
const RATE_CARD_PROBE_PPL: &str = "source=prices_effective | head 1 | eval one = 1 | fields one";

/// `matched_rate(…, "openai", "gpt-4o", 1000)` at 22:14:20.
const MATCHED_RATE_SQL: &str = "SELECT arrow_cast(p.input_usd_per_1m, 'Float64') AS input_rate, \
    arrow_cast(p.output_usd_per_1m, 'Float64') AS output_rate, \
    coalesce(arrow_cast(p.request_usd, 'Float64'), 0.0) AS request_usd \
    FROM iceberg.icegate.prices_effective p \
    WHERE p.provider = 'openai' AND p.model = 'gpt-4o' \
    AND p.service_tier = 'standard' AND p.region = 'global' \
    AND (p.min_input_tokens IS NULL OR 1000 >= p.min_input_tokens) \
    AND (p.max_input_tokens IS NULL OR 1000 < p.max_input_tokens) \
    AND to_timestamp_micros(1700000060000000) >= p.valid_from \
    AND (p.valid_to IS NULL OR to_timestamp_micros(1700000060000000) < p.valid_to) \
    ORDER BY p.valid_from DESC, p.min_input_tokens DESC LIMIT 1";

const MATCHED_RATE_PPL: &str = "source=prices_effective \
    | where provider = 'openai' and model = 'gpt-4o' and service_tier = 'standard' and region = 'global' and (isnull(min_input_tokens) or 1000 >= min_input_tokens) and (isnull(max_input_tokens) or 1000 < max_input_tokens) and timestamp('2023-11-14 22:14:20') >= valid_from and (isnull(valid_to) or timestamp('2023-11-14 22:14:20') < valid_to) \
    | sort - valid_from, - min_input_tokens \
    | head 1 \
    | eval input_rate = cast(input_usd_per_1m as double), output_rate = cast(output_usd_per_1m as double), request_usd = coalesce(cast(request_usd as double), 0.0D) \
    | fields input_rate, output_rate, request_usd";

/// The single cell of a one-row, one-column answer, whatever its column is named.
///
/// # Errors
///
/// Returns the answer when it is not one row of one column.
fn extract_single_cell(rows: &[Value]) -> Result<Value, String> {
    let [row] = rows else {
        return Err(format!("one row expected, got {rows:#?}"));
    };
    let cells: Vec<&Value> = row
        .as_object()
        .ok_or_else(|| format!("a row object expected, got {row}"))?
        .values()
        .collect();
    let [cell] = cells.as_slice() else {
        return Err(format!("one column expected, got {row}"));
    };
    Ok((*cell).clone())
}

/// On an empty catalog both probes answer one row: the constant `1`, and a zero count.
#[tokio::test]
async fn readiness_probe_counts_one_row_on_an_empty_table() {
    let (servers, _catalog) = ParityServers::start().await;
    assert_rows_in_order(
        "SQL",
        &servers.run_sql(TENANT_A, READINESS_SQL).await,
        &[json!({"one": 1})],
    );
    assert_rows_in_order(
        "PPL",
        &servers.run_ppl(TENANT_A, READINESS_PPL).await,
        &[json!({"n": 0})],
    );
    servers.shutdown().await;
}

/// Three spans of A and two of B in one snapshot: A counts three. trust's `scans` audit table is
/// empty: zero.
#[tokio::test]
async fn row_count_poll_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    let span_ids: [[u8; 8]; 5] = [[0x81; 8], [0x82; 8], [0x83; 8], [0x84; 8], [0x85; 8]];
    let rows: Vec<Row<'_>> = span_ids
        .iter()
        .zip([TENANT_A, TENANT_A, TENANT_A, TENANT_B, TENANT_B])
        .map(|(span_id, tenant)| Row {
            tenant,
            cells: vec![
                (COL_TRACE_ID, Cell::Bytes(&[0x80; 16])),
                (COL_SPAN_ID, Cell::Bytes(span_id)),
                (COL_TIMESTAMP, Cell::Micros(WINDOW_START_MICROS)),
                (COL_END_TIMESTAMP, Cell::Micros(WINDOW_START_MICROS + 1_000)),
                (COL_INGESTED_TIMESTAMP, Cell::Micros(WINDOW_START_MICROS)),
                (COL_DURATION_MICROS, Cell::Long(1_000)),
                (COL_NAME, Cell::Text("probe")),
            ],
        })
        .collect();
    commit_rows(&catalog, SPANS_TABLE, &rows).await.expect("seed spans");
    let expected_span_count = [json!({"n": 3})];
    assert_rows_in_order(
        "SQL spans",
        &servers.run_sql(TENANT_A, SPANS_COUNT_SQL).await,
        &expected_span_count,
    );
    assert_rows_in_order(
        "PPL spans",
        &servers.run_ppl(TENANT_A, SPANS_COUNT_PPL).await,
        &expected_span_count,
    );
    let expected_audit_count = [json!({"n": 0})];
    assert_rows_in_order(
        "SQL scans",
        &servers.run_sql(TENANT_A, SCANS_COUNT_SQL).await,
        &expected_audit_count,
    );
    assert_rows_in_order(
        "PPL scans",
        &servers.run_ppl(TENANT_A, SCANS_COUNT_PPL).await,
        &expected_audit_count,
    );
    servers.shutdown().await;
}

/// The rate card answers its probe with one row for either form: the SQL probe's one unnamed cell
/// is `1`, the PPL probe's one row is `one = 1`. The matched rate at 22:14:20
/// for 1000 input tokens is the current `gpt-4o` revision `2.0` / `8.0` (the orphaned `min 500`
/// tier is older and loses; the batch tier is filtered out) with no request fee; the rate card
/// is global, so tenant B reads the same rate.
#[tokio::test]
async fn prices_effective_probes_match_their_sql_forms() {
    let (servers, catalog) = ParityServers::start().await;
    commit_rate_card(&catalog).await;
    let sql_probe_cell =
        extract_single_cell(&servers.run_sql(TENANT_A, RATE_CARD_PROBE_SQL).await).expect("SQL probe answers one cell");
    assert_eq!(sql_probe_cell, json!(1), "SQL probe");
    assert_rows_in_order(
        "PPL probe",
        &servers.run_ppl(TENANT_A, RATE_CARD_PROBE_PPL).await,
        &[json!({"one": 1})],
    );
    let expected = [json!({"input_rate": 2.0, "output_rate": 8.0, "request_usd": 0.0})];
    for tenant in [TENANT_A, TENANT_B] {
        assert_rows_in_order(
            &format!("SQL {tenant}"),
            &servers.run_sql(tenant, MATCHED_RATE_SQL).await,
            &expected,
        );
        assert_rows_in_order(
            &format!("PPL {tenant}"),
            &servers.run_ppl(tenant, MATCHED_RATE_PPL).await,
            &expected,
        );
    }
    servers.shutdown().await;
}
