//! trust's Traces statements over `spans`: the trace list (window only, span-level filters,
//! trace-level `HAVING`, an id leaf, a keyset continuation), the trace header, the span
//! waterfall past ten thousand spans, and the facets.
//!
//! The seed holds five traces of tenant A and one root span of tenant B whose trace id equals
//! A's first trace: a leak would change that trace's span count, error and duration.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::SPANS_TABLE;
use icegate_common::schema::{
    COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_KIND, COL_NAME, COL_PARENT_SPAN_ID,
    COL_SERVICE_NAME, COL_SPAN_ATTRIBUTES, COL_SPAN_ID, COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TIMESTAMP,
    COL_TRACE_ID,
};
use serde_json::{Value, json};

use super::harness::{
    ParityServers, SECOND_MICROS, TENANT_A, TENANT_B, WINDOW_START_MICROS, assert_rows_in_order, build_facet_map,
    collect_ppl_facets, collect_sql_facets, render_timestamp,
};
use crate::opensearch::data::{Cell, Row, commit_rows};

/// `http.method` entries of a span.
const GET: &[(&str, &str)] = &[("http.method", "GET")];
const POST: &[(&str, &str)] = &[("http.method", "POST")];

/// One `spans` row of the seed; it starts `start_offset_micros` after the window start.
struct SpanLine {
    tenant: &'static str,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    service: &'static str,
    name: &'static str,
    start_offset_micros: i64,
    duration_micros: i64,
    status_code: i32,
    status_message: Option<&'static str>,
    attributes: &'static [(&'static str, &'static str)],
}

/// Traces `01…`, `02…`, `03…`, `05…` of A inside the window, `04…` of A two days before it, and
/// B's root span of trace `01…`.
static SPAN_LINES: [SpanLine; 7] = [
    SpanLine {
        tenant: TENANT_A,
        trace_id: [0x01; 16],
        span_id: [0xa1; 8],
        parent_span_id: None,
        service: "checkout",
        name: "GET /cart",
        start_offset_micros: 100 * SECOND_MICROS,
        duration_micros: 10_000,
        status_code: 0,
        status_message: None,
        attributes: GET,
    },
    SpanLine {
        tenant: TENANT_A,
        trace_id: [0x01; 16],
        span_id: [0xa2; 8],
        parent_span_id: Some([0xa1; 8]),
        service: "payments",
        name: "charge",
        start_offset_micros: 100 * SECOND_MICROS + 2_000,
        duration_micros: 6_000,
        status_code: 2,
        status_message: Some("card declined"),
        attributes: &[],
    },
    SpanLine {
        tenant: TENANT_A,
        trace_id: [0x02; 16],
        span_id: [0xb1; 8],
        parent_span_id: None,
        service: "checkout",
        name: "GET /home",
        start_offset_micros: 200 * SECOND_MICROS,
        duration_micros: 3_000,
        status_code: 0,
        status_message: None,
        attributes: GET,
    },
    SpanLine {
        tenant: TENANT_A,
        trace_id: [0x03; 16],
        span_id: [0xc1; 8],
        parent_span_id: None,
        service: "search",
        name: "POST /query",
        start_offset_micros: 300 * SECOND_MICROS,
        duration_micros: 20_000,
        status_code: 2,
        status_message: Some("timeout"),
        attributes: POST,
    },
    SpanLine {
        tenant: TENANT_A,
        trace_id: [0x05; 16],
        span_id: [0xe1; 8],
        parent_span_id: None,
        service: "search",
        name: "POST /query",
        start_offset_micros: 300 * SECOND_MICROS,
        duration_micros: 1_000,
        status_code: 0,
        status_message: None,
        attributes: POST,
    },
    SpanLine {
        tenant: TENANT_A,
        trace_id: [0x04; 16],
        span_id: [0xd1; 8],
        parent_span_id: None,
        service: "checkout",
        name: "GET /old",
        start_offset_micros: -172_800 * SECOND_MICROS,
        duration_micros: 1_000,
        status_code: 0,
        status_message: None,
        attributes: GET,
    },
    SpanLine {
        tenant: TENANT_B,
        trace_id: [0x01; 16],
        span_id: [0xa1; 8],
        parent_span_id: None,
        service: "checkout",
        name: "GET /cart",
        start_offset_micros: 100 * SECOND_MICROS,
        duration_micros: 50_000,
        status_code: 2,
        status_message: Some("b failure"),
        attributes: GET,
    },
];

/// One seed line as a `spans` row.
fn build_span_row(line: &'static SpanLine) -> Row<'static> {
    let start = WINDOW_START_MICROS + line.start_offset_micros;
    let mut cells = vec![
        (COL_SERVICE_NAME, Cell::Text(line.service)),
        (COL_TRACE_ID, Cell::Bytes(&line.trace_id)),
        (COL_SPAN_ID, Cell::Bytes(&line.span_id)),
        (COL_TIMESTAMP, Cell::Micros(start)),
        (COL_END_TIMESTAMP, Cell::Micros(start + line.duration_micros)),
        (COL_INGESTED_TIMESTAMP, Cell::Micros(start)),
        (COL_DURATION_MICROS, Cell::Long(line.duration_micros)),
        (COL_NAME, Cell::Text(line.name)),
        (COL_KIND, Cell::Int(2)),
        (COL_STATUS_CODE, Cell::Int(i64::from(line.status_code))),
        (COL_SPAN_ATTRIBUTES, Cell::Map(line.attributes)),
    ];
    if let Some(parent) = &line.parent_span_id {
        cells.push((COL_PARENT_SPAN_ID, Cell::Bytes(parent)));
    }
    if let Some(message) = line.status_message {
        cells.push((COL_STATUS_MESSAGE, Cell::Text(message)));
    }
    Row {
        tenant: line.tenant,
        cells,
    }
}

/// Commit the trace seed as one append.
async fn seed_spans(catalog: &Arc<dyn Catalog>) {
    let rows: Vec<Row<'static>> = SPAN_LINES.iter().map(build_span_row).collect();
    commit_rows(catalog, SPANS_TABLE, &rows).await.expect("seed spans");
}

// Expected trace summaries. `ended_at` is the latest span end; `has_error` is 1 when any span
// has status 2; `error_kind` is the greatest failed message.

fn build_expected_trace_one_row() -> Value {
    json!({"trace_id": "01010101010101010101010101010101", "service": "checkout", "operation": "GET /cart",
        "started_at": "2023-11-14 22:15:00", "ended_at": "2023-11-14 22:15:00.01", "span_count": 2,
        "has_error": 1, "error_kind": "card declined"})
}

fn build_expected_trace_two_row() -> Value {
    json!({"trace_id": "02020202020202020202020202020202", "service": "checkout", "operation": "GET /home",
        "started_at": "2023-11-14 22:16:40", "ended_at": "2023-11-14 22:16:40.003", "span_count": 1,
        "has_error": 0, "error_kind": null})
}

fn build_expected_trace_three_row() -> Value {
    json!({"trace_id": "03030303030303030303030303030303", "service": "search", "operation": "POST /query",
        "started_at": "2023-11-14 22:18:20", "ended_at": "2023-11-14 22:18:20.02", "span_count": 1,
        "has_error": 1, "error_kind": "timeout"})
}

fn build_expected_trace_five_row() -> Value {
    json!({"trace_id": "05050505050505050505050505050505", "service": "search", "operation": "POST /query",
        "started_at": "2023-11-14 22:18:20", "ended_at": "2023-11-14 22:18:20.001", "span_count": 1,
        "has_error": 0, "error_kind": null})
}

/// `trace_summaries_sql` for the first page over `[22:13:20, 23:13:20]`, 50 per page: the
/// matched CTE carries the strict window, the outer scan the window widened by one day.
const LIST_WINDOW_SQL: &str = r#"WITH "matched" AS (SELECT DISTINCT "trace_id" FROM "iceberg"."icegate"."spans" WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp <= to_timestamp_micros(1700003600000000)))
SELECT "trace_id", max(case when parent_span_id is null then service_name end) AS "service", max(case when parent_span_id is null then name end) AS "operation", min(timestamp) AS "started_at", max(end_timestamp) AS "ended_at", count(*) AS "span_count", max(case when status_code = 2 then 1 else 0 end) AS "has_error", max(case when status_code = 2 then status_message end) AS "error_kind"
FROM "iceberg"."icegate"."spans"
WHERE "trace_id" IN (SELECT "trace_id" FROM "matched") AND (timestamp >= to_timestamp_micros(1699913600000000)) AND (timestamp <= to_timestamp_micros(1700090000000000))
GROUP BY "trace_id"
ORDER BY "started_at" DESC, "trace_id" ASC
LIMIT 51"#;

const LIST_WINDOW_PPL: &str = "source=spans \
    | where `timestamp` >= timestamp('2023-11-13 22:13:20') and `timestamp` <= timestamp('2023-11-15 23:13:20') \
    | where trace_id in [ source=spans | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` <= timestamp('2023-11-14 23:13:20') | fields trace_id ] \
    | eval root_service = case(isnull(parent_span_id), service_name), root_name = case(isnull(parent_span_id), name), is_error = if(status_code = 2, 1, 0), error_message = case(status_code = 2, status_message) \
    | stats max(root_service) as service, max(root_name) as operation, min(`timestamp`) as started_at, max(end_timestamp) as ended_at, count() as span_count, max(is_error) as has_error, max(error_message) as error_kind by trace_id \
    | sort - started_at, trace_id \
    | head 51 \
    | fields trace_id, service, operation, started_at, ended_at, span_count, has_error, error_kind";

/// The first page under `?service=checkout` and the attribute filter `http.method = GET`,
/// both span-level conditions of the matched CTE.
const LIST_SPAN_FILTERS_SQL: &str = r#"WITH "matched" AS (SELECT DISTINCT "trace_id" FROM "iceberg"."icegate"."spans" WHERE "service_name" = 'checkout' AND (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp <= to_timestamp_micros(1700003600000000)) AND (array_element(map_extract(span_attributes, 'http.method'), 1) = 'GET'))
SELECT "trace_id", max(case when parent_span_id is null then service_name end) AS "service", max(case when parent_span_id is null then name end) AS "operation", min(timestamp) AS "started_at", max(end_timestamp) AS "ended_at", count(*) AS "span_count", max(case when status_code = 2 then 1 else 0 end) AS "has_error", max(case when status_code = 2 then status_message end) AS "error_kind"
FROM "iceberg"."icegate"."spans"
WHERE "trace_id" IN (SELECT "trace_id" FROM "matched") AND (timestamp >= to_timestamp_micros(1699913600000000)) AND (timestamp <= to_timestamp_micros(1700090000000000))
GROUP BY "trace_id"
ORDER BY "started_at" DESC, "trace_id" ASC
LIMIT 51"#;

const LIST_SPAN_FILTERS_PPL: &str = "source=spans \
    | where `timestamp` >= timestamp('2023-11-13 22:13:20') and `timestamp` <= timestamp('2023-11-15 23:13:20') \
    | where trace_id in [ source=spans | where service_name = 'checkout' and `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` <= timestamp('2023-11-14 23:13:20') and span_attributes.`http.method` = 'GET' | fields trace_id ] \
    | eval root_service = case(isnull(parent_span_id), service_name), root_name = case(isnull(parent_span_id), name), is_error = if(status_code = 2, 1, 0), error_message = case(status_code = 2, status_message) \
    | stats max(root_service) as service, max(root_name) as operation, min(`timestamp`) as started_at, max(end_timestamp) as ended_at, count() as span_count, max(is_error) as has_error, max(error_message) as error_kind by trace_id \
    | sort - started_at, trace_id \
    | head 51 \
    | fields trace_id, service, operation, started_at, ended_at, span_count, has_error, error_kind";

/// The first page under `?status=err&min_duration_ms=15`, both trace-level `HAVING` conditions.
const LIST_HAVING_SQL: &str = r#"WITH "matched" AS (SELECT DISTINCT "trace_id" FROM "iceberg"."icegate"."spans" WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp <= to_timestamp_micros(1700003600000000)))
SELECT "trace_id", max(case when parent_span_id is null then service_name end) AS "service", max(case when parent_span_id is null then name end) AS "operation", min(timestamp) AS "started_at", max(end_timestamp) AS "ended_at", count(*) AS "span_count", max(case when status_code = 2 then 1 else 0 end) AS "has_error", max(case when status_code = 2 then status_message end) AS "error_kind"
FROM "iceberg"."icegate"."spans"
WHERE "trace_id" IN (SELECT "trace_id" FROM "matched") AND (timestamp >= to_timestamp_micros(1699913600000000)) AND (timestamp <= to_timestamp_micros(1700090000000000))
GROUP BY "trace_id"
HAVING max(case when status_code = 2 then 1 else 0 end) = 1 AND greatest(coalesce((arrow_cast(max(end_timestamp), 'Int64') - arrow_cast(min(timestamp), 'Int64')) / 1000, 0), 0) >= 15
ORDER BY "started_at" DESC, "trace_id" ASC
LIMIT 51"#;

const LIST_HAVING_PPL: &str = "source=spans \
    | where `timestamp` >= timestamp('2023-11-13 22:13:20') and `timestamp` <= timestamp('2023-11-15 23:13:20') \
    | where trace_id in [ source=spans | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` <= timestamp('2023-11-14 23:13:20') | fields trace_id ] \
    | eval root_service = case(isnull(parent_span_id), service_name), root_name = case(isnull(parent_span_id), name), is_error = if(status_code = 2, 1, 0), error_message = case(status_code = 2, status_message) \
    | stats max(root_service) as service, max(root_name) as operation, min(`timestamp`) as started_at, max(end_timestamp) as ended_at, count() as span_count, max(is_error) as has_error, max(error_message) as error_kind by trace_id \
    | eval duration_ms = timestampdiff(MICROSECOND, started_at, ended_at) / 1000 \
    | where has_error = 1 and if(duration_ms < 0, 0, duration_ms) >= 15 \
    | sort - started_at, trace_id \
    | head 51 \
    | fields trace_id, service, operation, started_at, ended_at, span_count, has_error, error_kind";

/// The first page under the filter leaf `trace_id = 0101…01` (`list_advanced_predicate`): the
/// group key compared in `HAVING`.
const LIST_ID_LEAF_SQL: &str = r#"WITH "matched" AS (SELECT DISTINCT "trace_id" FROM "iceberg"."icegate"."spans" WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp <= to_timestamp_micros(1700003600000000)))
SELECT "trace_id", max(case when parent_span_id is null then service_name end) AS "service", max(case when parent_span_id is null then name end) AS "operation", min(timestamp) AS "started_at", max(end_timestamp) AS "ended_at", count(*) AS "span_count", max(case when status_code = 2 then 1 else 0 end) AS "has_error", max(case when status_code = 2 then status_message end) AS "error_kind"
FROM "iceberg"."icegate"."spans"
WHERE "trace_id" IN (SELECT "trace_id" FROM "matched") AND (timestamp >= to_timestamp_micros(1699913600000000)) AND (timestamp <= to_timestamp_micros(1700090000000000))
GROUP BY "trace_id"
HAVING "trace_id" = arrow_cast(decode('01010101010101010101010101010101', 'hex'), 'FixedSizeBinary(16)')
ORDER BY "started_at" DESC, "trace_id" ASC
LIMIT 51"#;

const LIST_ID_LEAF_PPL: &str = "source=spans \
    | where `timestamp` >= timestamp('2023-11-13 22:13:20') and `timestamp` <= timestamp('2023-11-15 23:13:20') \
    | where trace_id in [ source=spans | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` <= timestamp('2023-11-14 23:13:20') | fields trace_id ] \
    | eval root_service = case(isnull(parent_span_id), service_name), root_name = case(isnull(parent_span_id), name), is_error = if(status_code = 2, 1, 0), error_message = case(status_code = 2, status_message) \
    | stats max(root_service) as service, max(root_name) as operation, min(`timestamp`) as started_at, max(end_timestamp) as ended_at, count() as span_count, max(is_error) as has_error, max(error_message) as error_kind by trace_id \
    | where trace_id = '01010101010101010101010101010101' \
    | sort - started_at, trace_id \
    | head 51 \
    | fields trace_id, service, operation, started_at, ended_at, span_count, has_error, error_kind";

/// The continuation after `(22:18:20, 0303…03)` with one trace per page (`LIMIT 2`): the tie
/// on `started_at` is broken by the trace id in byte order.
const LIST_KEYSET_SQL: &str = r#"WITH "matched" AS (SELECT DISTINCT "trace_id" FROM "iceberg"."icegate"."spans" WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp <= to_timestamp_micros(1700003600000000)))
SELECT "trace_id", max(case when parent_span_id is null then service_name end) AS "service", max(case when parent_span_id is null then name end) AS "operation", min(timestamp) AS "started_at", max(end_timestamp) AS "ended_at", count(*) AS "span_count", max(case when status_code = 2 then 1 else 0 end) AS "has_error", max(case when status_code = 2 then status_message end) AS "error_kind"
FROM "iceberg"."icegate"."spans"
WHERE "trace_id" IN (SELECT "trace_id" FROM "matched") AND (timestamp >= to_timestamp_micros(1699913600000000)) AND (timestamp <= to_timestamp_micros(1700090000000000))
GROUP BY "trace_id"
HAVING (min(timestamp) < to_timestamp_micros(1700000300000000) OR (min(timestamp) = to_timestamp_micros(1700000300000000) AND trace_id > arrow_cast(decode('03030303030303030303030303030303', 'hex'), 'FixedSizeBinary(16)')))
ORDER BY "started_at" DESC, "trace_id" ASC
LIMIT 2"#;

const LIST_KEYSET_PPL: &str = "source=spans \
    | where `timestamp` >= timestamp('2023-11-13 22:13:20') and `timestamp` <= timestamp('2023-11-15 23:13:20') \
    | where trace_id in [ source=spans | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` <= timestamp('2023-11-14 23:13:20') | fields trace_id ] \
    | eval root_service = case(isnull(parent_span_id), service_name), root_name = case(isnull(parent_span_id), name), is_error = if(status_code = 2, 1, 0), error_message = case(status_code = 2, status_message) \
    | stats max(root_service) as service, max(root_name) as operation, min(`timestamp`) as started_at, max(end_timestamp) as ended_at, count() as span_count, max(is_error) as has_error, max(error_message) as error_kind by trace_id \
    | where started_at < timestamp('2023-11-14 22:18:20') or (started_at = timestamp('2023-11-14 22:18:20') and trace_id > '03030303030303030303030303030303') \
    | sort - started_at, trace_id \
    | head 2 \
    | fields trace_id, service, operation, started_at, ended_at, span_count, has_error, error_kind";

/// `trace_summaries_sql` for `find(0101…01)`: no window, no limit.
const HEADER_SQL: &str = r#"WITH "matched" AS (SELECT DISTINCT "trace_id" FROM "iceberg"."icegate"."spans" WHERE "trace_id" = arrow_cast(decode('01010101010101010101010101010101', 'hex'), 'FixedSizeBinary(16)'))
SELECT "trace_id", max(case when parent_span_id is null then service_name end) AS "service", max(case when parent_span_id is null then name end) AS "operation", min(timestamp) AS "started_at", max(end_timestamp) AS "ended_at", count(*) AS "span_count", max(case when status_code = 2 then 1 else 0 end) AS "has_error", max(case when status_code = 2 then status_message end) AS "error_kind"
FROM "iceberg"."icegate"."spans"
WHERE "trace_id" IN (SELECT "trace_id" FROM "matched")
GROUP BY "trace_id"
ORDER BY "started_at" DESC, "trace_id" ASC"#;

const HEADER_PPL: &str = "source=spans \
    | where trace_id = '01010101010101010101010101010101' \
    | eval root_service = case(isnull(parent_span_id), service_name), root_name = case(isnull(parent_span_id), name), is_error = if(status_code = 2, 1, 0), error_message = case(status_code = 2, status_message) \
    | stats max(root_service) as service, max(root_name) as operation, min(`timestamp`) as started_at, max(end_timestamp) as ended_at, count() as span_count, max(is_error) as has_error, max(error_message) as error_kind by trace_id \
    | sort - started_at, trace_id \
    | fields trace_id, service, operation, started_at, ended_at, span_count, has_error, error_kind";

/// The waterfall trace: 10 005 spans of A, one millisecond apart.
const WATERFALL_TRACE_ID: [u8; 16] = [0x0a; 16];
/// [`WATERFALL_TRACE_ID`] as lowercase hex.
const WATERFALL_TRACE_HEX: &str = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a";
/// Spans A writes into the waterfall trace: more than the `head 10001` the statement asks for.
const WATERFALL_SPANS: usize = 10_005;
/// Rows the waterfall statement returns: its cap of 10 000 plus the one that shows clipping.
const WATERFALL_ROWS: usize = 10_001;
/// B's spans of the same trace id, all earlier than every A span.
const TENANT_B_SPAN_IDS: [[u8; 8]; 3] = [[0xb1; 8], [0xb2; 8], [0xb3; 8]];

/// `spans_for_trace_sql(0a0a…0a, None)`.
const WATERFALL_SQL: &str = r#"SELECT "s"."service_name", "s"."trace_id", "s"."span_id", "s"."parent_span_id", "s"."timestamp", "s"."end_timestamp", "s"."ingested_timestamp", "s"."duration_micros", "s"."trace_state", "s"."name", "s"."kind", "s"."status_code", "s"."status_message", "s"."resource_attributes", "s"."span_attributes", "s"."scope_attributes", "s"."flags", "s"."dropped_attributes_count", "s"."dropped_events_count", "s"."dropped_links_count", "s"."events"
FROM "iceberg"."icegate"."spans" AS "s"
WHERE "s"."trace_id" = arrow_cast(decode('0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a', 'hex'), 'FixedSizeBinary(16)')
ORDER BY "s"."timestamp" ASC
LIMIT 10001"#;

const WATERFALL_PPL: &str = "source=spans \
    | where trace_id = '0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a' \
    | sort `timestamp` \
    | head 10001 \
    | fields service_name, trace_id, span_id, parent_span_id, `timestamp`, end_timestamp, ingested_timestamp, duration_micros, trace_state, name, kind, status_code, status_message, resource_attributes, span_attributes, scope_attributes, flags, dropped_attributes_count, dropped_events_count, dropped_links_count, events";

/// `spans_for_trace_sql(0a0a…0a, Some(0000000000000008))`: the deep link to one span.
const DEEP_LINK_SQL: &str = r#"SELECT "s"."service_name", "s"."trace_id", "s"."span_id", "s"."parent_span_id", "s"."timestamp", "s"."end_timestamp", "s"."ingested_timestamp", "s"."duration_micros", "s"."trace_state", "s"."name", "s"."kind", "s"."status_code", "s"."status_message", "s"."resource_attributes", "s"."span_attributes", "s"."scope_attributes", "s"."flags", "s"."dropped_attributes_count", "s"."dropped_events_count", "s"."dropped_links_count", "s"."events"
FROM "iceberg"."icegate"."spans" AS "s"
WHERE "s"."trace_id" = arrow_cast(decode('0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a', 'hex'), 'FixedSizeBinary(16)') AND "s"."span_id" = arrow_cast(decode('0000000000000008', 'hex'), 'FixedSizeBinary(8)')
ORDER BY "s"."timestamp" ASC
LIMIT 1"#;

const DEEP_LINK_PPL: &str = "source=spans \
    | where trace_id = '0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a' and span_id = '0000000000000008' \
    | sort `timestamp` \
    | head 1 \
    | fields service_name, trace_id, span_id, parent_span_id, `timestamp`, end_timestamp, ingested_timestamp, duration_micros, trace_state, name, kind, status_code, status_message, resource_attributes, span_attributes, scope_attributes, flags, dropped_attributes_count, dropped_events_count, dropped_links_count, events";

/// Start of waterfall span `index` (0-based): the window start plus `index` milliseconds.
fn compute_waterfall_start(index: usize) -> i64 {
    WINDOW_START_MICROS + i64::try_from(index).expect("index fits i64") * 1_000
}

/// One waterfall `spans` row: 500 µs long, kind 1, status 0, service `svc`.
fn build_waterfall_span<'a>(tenant: &'a str, start: i64, name: &'a str, span_id: &'a [u8; 8]) -> Row<'a> {
    Row {
        tenant,
        cells: vec![
            (COL_SERVICE_NAME, Cell::Text("svc")),
            (COL_TRACE_ID, Cell::Bytes(&WATERFALL_TRACE_ID)),
            (COL_SPAN_ID, Cell::Bytes(span_id)),
            (COL_TIMESTAMP, Cell::Micros(start)),
            (COL_END_TIMESTAMP, Cell::Micros(start + 500)),
            (COL_INGESTED_TIMESTAMP, Cell::Micros(start)),
            (COL_DURATION_MICROS, Cell::Long(500)),
            (COL_NAME, Cell::Text(name)),
            (COL_KIND, Cell::Int(1)),
            (COL_STATUS_CODE, Cell::Int(0)),
        ],
    }
}

/// The expected waterfall row of span `index`; its span id is `index + 1` big-endian.
fn build_expected_waterfall_row(index: usize) -> Value {
    let start = compute_waterfall_start(index);
    json!({
        "service_name": "svc", "trace_id": WATERFALL_TRACE_HEX, "span_id": format!("{:016x}", index + 1),
        "parent_span_id": null, "timestamp": render_timestamp(start), "end_timestamp": render_timestamp(start + 500),
        "ingested_timestamp": render_timestamp(start), "duration_micros": 500, "trace_state": null,
        "name": format!("op-{index}"), "kind": 1, "status_code": 0, "status_message": null,
        "resource_attributes": {}, "span_attributes": {}, "scope_attributes": {}, "flags": null,
        "dropped_attributes_count": null, "dropped_events_count": null, "dropped_links_count": null,
        "events": null
    })
}

/// `facets_sql(window, {service, error_kind})`: `v0` is the root span's service, `v1` a failed
/// span's message.
const FACETS_SQL: &str = r#"WITH src AS (SELECT CASE WHEN parent_span_id IS NULL THEN nullif(service_name, '') END AS "v0", CASE WHEN status_code = 2 THEN nullif(status_message, '') END AS "v1" FROM "iceberg"."icegate"."spans" WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000))),
g AS (SELECT v0, v1 FROM src GROUP BY GROUPING SETS ((v0), (v1))),
u AS (SELECT CASE WHEN v0 IS NOT NULL THEN 'service' WHEN v1 IS NOT NULL THEN 'error_kind' END AS field, coalesce(v0, v1) AS value FROM g)
SELECT field, value FROM (SELECT field, value, row_number() OVER (PARTITION BY field ORDER BY value) AS rn FROM u WHERE value IS NOT NULL) t WHERE rn <= 201"#;

/// The PPL form names "no value" with `case` without `else`, never with a bare `null`.
const FACETS_PPL: &str = "source=spans \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | eval v0 = case(isnull(parent_span_id), nullif(service_name, '')), v1 = case(status_code = 2, nullif(status_message, '')) \
    | stats values(v0) as v0, values(v1) as v1 \
    | eval v0 = mvindex(v0, 0, 200), v1 = mvindex(v1, 0, 200)";

/// A bare `null` is an identifier like any other, and no field carries that name.
const BARE_NULL_PPL: &str = "source=spans \
    | eval v0 = if(isnull(parent_span_id), nullif(service_name, ''), null) \
    | stats values(v0) as v0";

/// Window only: four traces of A, newest first, ties by trace id. Span-level filters keep the
/// two `checkout` traces with a `GET` span. The `HAVING` keeps the failed trace of at least
/// 15 ms: `03…` (20 ms), not `01…` (10 ms). B's span of trace `01…` (50 ms, failed with
/// `b failure`) would change trace `01…` in every case if it leaked.
#[tokio::test]
async fn trace_list_window_span_filters_and_having_match_their_sql_forms() {
    let (servers, catalog) = ParityServers::start().await;
    seed_spans(&catalog).await;
    let cases = [
        (
            "window only",
            LIST_WINDOW_SQL,
            LIST_WINDOW_PPL,
            vec![
                build_expected_trace_three_row(),
                build_expected_trace_five_row(),
                build_expected_trace_two_row(),
                build_expected_trace_one_row(),
            ],
        ),
        (
            "span-level filters",
            LIST_SPAN_FILTERS_SQL,
            LIST_SPAN_FILTERS_PPL,
            vec![build_expected_trace_two_row(), build_expected_trace_one_row()],
        ),
        (
            "trace-level having",
            LIST_HAVING_SQL,
            LIST_HAVING_PPL,
            vec![build_expected_trace_three_row()],
        ),
    ];
    for (case, sql, ppl, expected) in cases {
        assert_rows_in_order(&format!("SQL {case}"), &servers.run_sql(TENANT_A, sql).await, &expected);
        assert_rows_in_order(&format!("PPL {case}"), &servers.run_ppl(TENANT_A, ppl).await, &expected);
    }
    servers.shutdown().await;
}

/// The id leaf keeps trace `01…` alone; the continuation after trace `03…` starts with `05…`,
/// which shares its start, and then `02…`.
#[tokio::test]
async fn trace_list_with_id_leaves_and_keyset_continuation_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_spans(&catalog).await;
    let cases = [
        (
            "id leaf",
            LIST_ID_LEAF_SQL,
            LIST_ID_LEAF_PPL,
            vec![build_expected_trace_one_row()],
        ),
        (
            "keyset continuation",
            LIST_KEYSET_SQL,
            LIST_KEYSET_PPL,
            vec![build_expected_trace_five_row(), build_expected_trace_two_row()],
        ),
    ];
    for (case, sql, ppl, expected) in cases {
        assert_rows_in_order(&format!("SQL {case}"), &servers.run_sql(TENANT_A, sql).await, &expected);
        assert_rows_in_order(&format!("PPL {case}"), &servers.run_ppl(TENANT_A, ppl).await, &expected);
    }
    servers.shutdown().await;
}

/// The header covers both spans of A's trace `01…` and none of B's.
#[tokio::test]
async fn trace_header_by_id_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_spans(&catalog).await;
    let expected = [build_expected_trace_one_row()];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, HEADER_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, HEADER_PPL).await, &expected);
    servers.shutdown().await;
}

/// 10 005 spans of A and 3 earlier spans of B share the trace id; both forms return A's first
/// 10 001 spans in start order, so the reader sees the clip. The deep link returns span 8.
#[tokio::test]
async fn span_waterfall_over_ten_thousand_rows_matches_with_head_10001() {
    let (servers, catalog) = ParityServers::start().await;
    let names: Vec<String> = (0..WATERFALL_SPANS).map(|index| format!("op-{index}")).collect();
    let span_ids: Vec<[u8; 8]> = (1..=WATERFALL_SPANS)
        .map(|number| u64::try_from(number).expect("span number fits u64").to_be_bytes())
        .collect();
    let mut rows: Vec<Row<'_>> = names
        .iter()
        .zip(&span_ids)
        .enumerate()
        .map(|(index, (name, span_id))| build_waterfall_span(TENANT_A, compute_waterfall_start(index), name, span_id))
        .collect();
    for (offset, span_id) in (1_i64..).zip(&TENANT_B_SPAN_IDS) {
        rows.push(build_waterfall_span(
            TENANT_B,
            WINDOW_START_MICROS - offset * 1_000,
            "b-op",
            span_id,
        ));
    }
    commit_rows(&catalog, SPANS_TABLE, &rows).await.expect("seed the waterfall");

    let expected: Vec<Value> = (0..WATERFALL_ROWS).map(build_expected_waterfall_row).collect();
    assert_rows_in_order(
        "SQL waterfall",
        &servers.run_sql(TENANT_A, WATERFALL_SQL).await,
        &expected,
    );
    assert_rows_in_order(
        "PPL waterfall",
        &servers.run_ppl(TENANT_A, WATERFALL_PPL).await,
        &expected,
    );
    let deep_link = [build_expected_waterfall_row(7)];
    assert_rows_in_order(
        "SQL deep link",
        &servers.run_sql(TENANT_A, DEEP_LINK_SQL).await,
        &deep_link,
    );
    assert_rows_in_order(
        "PPL deep link",
        &servers.run_ppl(TENANT_A, DEEP_LINK_PPL).await,
        &deep_link,
    );
    servers.shutdown().await;
}

/// Root services and failed messages of A's window: the child span's `payments` is no root,
/// trace `04…` is outside the window, and B's `b failure` is another tenant's. The PPL form
/// spells "no value" as `case` without `else`; the same field written with a bare `null` is a
/// 400 naming a missing field.
#[tokio::test]
async fn trace_facets_values_of_case_without_else_match_the_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_spans(&catalog).await;
    let expected = build_facet_map(&[
        ("error_kind", &["card declined", "timeout"][..]),
        ("service", &["checkout", "search"][..]),
    ]);
    assert_eq!(
        collect_sql_facets(&servers.run_sql(TENANT_A, FACETS_SQL).await),
        expected,
        "SQL"
    );
    let ppl_rows = servers.run_ppl(TENANT_A, FACETS_PPL).await;
    assert_eq!(
        collect_ppl_facets(&ppl_rows, &[("v0", "service"), ("v1", "error_kind")]),
        expected,
        "PPL"
    );
    let reply = servers.post_ppl(TENANT_A, BARE_NULL_PPL).await;
    assert_eq!(reply.status, 400, "{}", reply.text);
    assert_eq!(reply.content_type, "application/json; charset=UTF-8", "{}", reply.text);
    let error = reply.parse_json_body();
    assert_eq!(error["error"]["type"], "SemanticCheckException", "{error}");
    assert_eq!(error["error"]["code"], "FIELD_NOT_FOUND", "{error}");
    assert_eq!(error["error"]["context"]["field"], "null", "{error}");
    servers.shutdown().await;
}
