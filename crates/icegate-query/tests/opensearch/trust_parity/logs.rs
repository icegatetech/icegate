//! trust's Logs statements over `logs`: the list page and its keyset continuation, the list
//! filtered on a trace id, the level histogram, the locator lookup, the context around an anchor
//! and the facets.
//!
//! One seed serves every case: seven lines of tenant A inside the window `[22:13:20, 23:13:20)`,
//! one line of A just before it, and three lines of tenant B: one identical to an A line except
//! for its tenant, one on A's `(checkout, pod-b)` context stream after A's anchor, and one
//! of its own service.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::LOGS_TABLE;
use icegate_common::schema::{
    COL_BODY, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES, COL_OBSERVED_TIMESTAMP, COL_RESOURCE_ATTRIBUTES,
    COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID,
};
use serde_json::{Value, json};

use super::harness::{
    ParityServers, SECOND_MICROS, TENANT_A, TENANT_B, WINDOW_START_MICROS, assert_rows_in_order, assert_rows_unordered,
    build_facet_map, collect_ppl_facets, collect_sql_facets,
};
use crate::opensearch::data::{Cell, Row, commit_rows};

/// The trace id of the failed payment line, shared by both tenants.
const PAYMENT_TRACE_ID: [u8; 16] = [0x11; 16];
/// The span id of the failed payment line, shared by both tenants.
const PAYMENT_SPAN_ID: [u8; 8] = [0x22; 8];

/// `host.name` entries of one attribute map.
const POD_A: &[(&str, &str)] = &[("host.name", "pod-a")];
const POD_B: &[(&str, &str)] = &[("host.name", "pod-b")];
const POD_C: &[(&str, &str)] = &[("host.name", "pod-c")];
const POD_Z: &[(&str, &str)] = &[("host.name", "pod-z")];

/// One `logs` row of the seed. Its timestamp, observed and ingested timestamps are all the window
/// start plus `offset_seconds`.
struct LogLine {
    tenant: &'static str,
    offset_seconds: i64,
    service: Option<&'static str>,
    /// The attribute column carrying `host.name`, and its entries.
    host: Option<(&'static str, &'static [(&'static str, &'static str)])>,
    severity: &'static str,
    body: &'static str,
    /// Whether the line carries the payment trace and span ids.
    is_payment: bool,
}

/// The seed, by tenant and time.
static LOG_LINES: [LogLine; 11] = [
    LogLine {
        tenant: TENANT_A,
        offset_seconds: 5,
        service: Some("checkout"),
        host: Some((COL_RESOURCE_ATTRIBUTES, POD_B)),
        severity: "INFO",
        body: "cart opened",
        is_payment: false,
    },
    LogLine {
        tenant: TENANT_A,
        offset_seconds: 10,
        service: Some("checkout"),
        host: Some((COL_LOG_ATTRIBUTES, POD_A)),
        severity: "INFO",
        body: "order placed",
        is_payment: false,
    },
    LogLine {
        tenant: TENANT_A,
        offset_seconds: 20,
        service: Some("checkout"),
        host: Some((COL_RESOURCE_ATTRIBUTES, POD_B)),
        severity: "ERROR",
        body: "payment failed",
        is_payment: true,
    },
    LogLine {
        tenant: TENANT_A,
        offset_seconds: 20,
        service: Some("checkout"),
        host: Some((COL_RESOURCE_ATTRIBUTES, POD_B)),
        severity: "INFO",
        body: "payment retried",
        is_payment: false,
    },
    LogLine {
        tenant: TENANT_A,
        offset_seconds: 20,
        service: Some("billing"),
        host: None,
        severity: "WARN",
        body: "retrying",
        is_payment: false,
    },
    LogLine {
        tenant: TENANT_A,
        offset_seconds: 30,
        service: None,
        host: Some((COL_SCOPE_ATTRIBUTES, POD_C)),
        severity: "debug",
        body: "cache miss",
        is_payment: false,
    },
    LogLine {
        tenant: TENANT_A,
        offset_seconds: 40,
        service: Some("checkout"),
        host: Some((COL_RESOURCE_ATTRIBUTES, POD_B)),
        severity: "INFO",
        body: "receipt sent",
        is_payment: false,
    },
    LogLine {
        tenant: TENANT_A,
        offset_seconds: -1,
        service: Some("checkout"),
        host: Some((COL_RESOURCE_ATTRIBUTES, POD_B)),
        severity: "INFO",
        body: "too early",
        is_payment: false,
    },
    LogLine {
        tenant: TENANT_B,
        offset_seconds: 20,
        service: Some("checkout"),
        host: Some((COL_RESOURCE_ATTRIBUTES, POD_B)),
        severity: "ERROR",
        body: "payment failed",
        is_payment: true,
    },
    LogLine {
        tenant: TENANT_B,
        offset_seconds: 25,
        service: Some("b-service"),
        host: Some((COL_RESOURCE_ATTRIBUTES, POD_Z)),
        severity: "INFO",
        body: "tenant b only",
        is_payment: false,
    },
    LogLine {
        tenant: TENANT_B,
        offset_seconds: 45,
        service: Some("checkout"),
        host: Some((COL_RESOURCE_ATTRIBUTES, POD_B)),
        severity: "INFO",
        body: "b newer",
        is_payment: false,
    },
];

/// One seed line as a `logs` row.
fn build_log_row(line: &'static LogLine) -> Row<'static> {
    let timestamp_micros = WINDOW_START_MICROS + line.offset_seconds * SECOND_MICROS;
    let mut cells = vec![
        (COL_TIMESTAMP, Cell::Micros(timestamp_micros)),
        (COL_OBSERVED_TIMESTAMP, Cell::Micros(timestamp_micros)),
        (COL_INGESTED_TIMESTAMP, Cell::Micros(timestamp_micros)),
        (COL_SEVERITY_TEXT, Cell::Text(line.severity)),
        (COL_BODY, Cell::Text(line.body)),
    ];
    if let Some(service) = line.service {
        cells.push((COL_SERVICE_NAME, Cell::Text(service)));
    }
    if let Some((column, entries)) = line.host {
        cells.push((column, Cell::Map(entries)));
    }
    if line.is_payment {
        cells.push((COL_TRACE_ID, Cell::Bytes(&PAYMENT_TRACE_ID)));
        cells.push((COL_SPAN_ID, Cell::Bytes(&PAYMENT_SPAN_ID)));
    }
    Row {
        tenant: line.tenant,
        cells,
    }
}

/// Commit the whole seed as one append: both tenants' rows in one batch.
async fn seed_logs(catalog: &Arc<dyn Catalog>) {
    let rows: Vec<Row<'static>> = LOG_LINES.iter().map(build_log_row).collect();
    commit_rows(catalog, LOGS_TABLE, &rows).await.expect("seed logs");
}

// Expected records, in the list projection. `body_hash` is `md5(body)`, computed outside the code.

fn build_expected_cart_opened_row() -> Value {
    json!({
        "service_name": "checkout", "timestamp": "2023-11-14 22:13:25", "observed_timestamp": "2023-11-14 22:13:25",
        "severity_text": "INFO", "body": "cart opened", "resource_attributes": {"host.name": "pod-b"},
        "scope_attributes": {}, "log_attributes": {}, "trace_id": null, "span_id": null,
        "body_hash": "c39157c727d821026c3c2a48f373834f"
    })
}

fn build_expected_order_placed_row() -> Value {
    json!({
        "service_name": "checkout", "timestamp": "2023-11-14 22:13:30", "observed_timestamp": "2023-11-14 22:13:30",
        "severity_text": "INFO", "body": "order placed", "resource_attributes": {},
        "scope_attributes": {}, "log_attributes": {"host.name": "pod-a"}, "trace_id": null, "span_id": null,
        "body_hash": "8a6d9d2ea0d85d7399e44045975a05a0"
    })
}

fn build_expected_payment_failed_row() -> Value {
    json!({
        "service_name": "checkout", "timestamp": "2023-11-14 22:13:40", "observed_timestamp": "2023-11-14 22:13:40",
        "severity_text": "ERROR", "body": "payment failed", "resource_attributes": {"host.name": "pod-b"},
        "scope_attributes": {}, "log_attributes": {}, "trace_id": "11111111111111111111111111111111",
        "span_id": "2222222222222222", "body_hash": "c52e73296e544682fed91beabd0ca8c1"
    })
}

fn build_expected_payment_retried_row() -> Value {
    json!({
        "service_name": "checkout", "timestamp": "2023-11-14 22:13:40", "observed_timestamp": "2023-11-14 22:13:40",
        "severity_text": "INFO", "body": "payment retried", "resource_attributes": {"host.name": "pod-b"},
        "scope_attributes": {}, "log_attributes": {}, "trace_id": null, "span_id": null,
        "body_hash": "dd4e28b1ea7ac85f88721fbb86d743e6"
    })
}

fn build_expected_retrying_row() -> Value {
    json!({
        "service_name": "billing", "timestamp": "2023-11-14 22:13:40", "observed_timestamp": "2023-11-14 22:13:40",
        "severity_text": "WARN", "body": "retrying", "resource_attributes": {},
        "scope_attributes": {}, "log_attributes": {}, "trace_id": null, "span_id": null,
        "body_hash": "d7c09b673d244e3de8e0c48a9002112f"
    })
}

fn build_expected_cache_miss_row() -> Value {
    json!({
        "service_name": null, "timestamp": "2023-11-14 22:13:50", "observed_timestamp": "2023-11-14 22:13:50",
        "severity_text": "debug", "body": "cache miss", "resource_attributes": {},
        "scope_attributes": {"host.name": "pod-c"}, "log_attributes": {}, "trace_id": null, "span_id": null,
        "body_hash": "db820e6cc39c5392a41cbc4dc7831cf4"
    })
}

fn build_expected_receipt_sent_row() -> Value {
    json!({
        "service_name": "checkout", "timestamp": "2023-11-14 22:14:00", "observed_timestamp": "2023-11-14 22:14:00",
        "severity_text": "INFO", "body": "receipt sent", "resource_attributes": {"host.name": "pod-b"},
        "scope_attributes": {}, "log_attributes": {}, "trace_id": null, "span_id": null,
        "body_hash": "e8faac582f828e3d18f14b9e2fbf1abc"
    })
}

/// `record` with the context branch marker the context statement adds.
fn tag_row_kind(mut record: Value, kind: &str) -> Value {
    record["context_row_kind"] = json!(kind);
    record
}

/// `logs_list_sql(from, to, None, None, 4)`: page size 3, so `LIMIT 4`.
const LIST_FIRST_PAGE_SQL: &str = r#"SELECT "service_name", "timestamp", "observed_timestamp", "severity_text", "body", "resource_attributes", "scope_attributes", "log_attributes", "trace_id", "span_id", coalesce(md5(body), '') AS "body_hash"
FROM "iceberg"."icegate"."logs"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000))
ORDER BY timestamp DESC, coalesce(service_name, '') ASC, coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') ASC, coalesce(md5(body), '') ASC
LIMIT 4"#;

const LIST_FIRST_PAGE_PPL: &str = "source=logs \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') \
    | sort - `timestamp`, svc, host, body_hash \
    | head 4 \
    | fields service_name, `timestamp`, observed_timestamp, severity_text, body, resource_attributes, scope_attributes, log_attributes, trace_id, span_id, body_hash";

/// The continuation after the first page's last served row, `retrying`:
/// `(22:13:40, 'billing', '', 'd7c09b673d244e3de8e0c48a9002112f')`.
const LIST_CONTINUATION_SQL: &str = r#"SELECT "service_name", "timestamp", "observed_timestamp", "severity_text", "body", "resource_attributes", "scope_attributes", "log_attributes", "trace_id", "span_id", coalesce(md5(body), '') AS "body_hash"
FROM "iceberg"."icegate"."logs"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000))
AND (timestamp < to_timestamp_micros(1700000020000000) OR (timestamp = to_timestamp_micros(1700000020000000) AND ( coalesce(service_name, '') > 'billing' OR (coalesce(service_name, '') = 'billing' AND coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') > '') OR (coalesce(service_name, '') = 'billing' AND coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') = '' AND coalesce(md5(body), '') > 'd7c09b673d244e3de8e0c48a9002112f'))))
ORDER BY timestamp DESC, coalesce(service_name, '') ASC, coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') ASC, coalesce(md5(body), '') ASC
LIMIT 4"#;

const LIST_CONTINUATION_PPL: &str = "source=logs \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') \
    | where `timestamp` < timestamp('2023-11-14 22:13:40') or (`timestamp` = timestamp('2023-11-14 22:13:40') and (svc > 'billing' or (svc = 'billing' and host > '') or (svc = 'billing' and host = '' and body_hash > 'd7c09b673d244e3de8e0c48a9002112f'))) \
    | sort - `timestamp`, svc, host, body_hash \
    | head 4 \
    | fields service_name, `timestamp`, observed_timestamp, severity_text, body, resource_attributes, scope_attributes, log_attributes, trace_id, span_id, body_hash";

/// The first page under the filter leaf `trace_id = 1111…11` (the `first_page_filtered` id).
const LIST_TRACE_FILTER_SQL: &str = r#"SELECT "service_name", "timestamp", "observed_timestamp", "severity_text", "body", "resource_attributes", "scope_attributes", "log_attributes", "trace_id", "span_id", coalesce(md5(body), '') AS "body_hash"
FROM "iceberg"."icegate"."logs"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000))
AND "trace_id" = arrow_cast(decode('11111111111111111111111111111111', 'hex'), 'FixedSizeBinary(16)')
ORDER BY timestamp DESC, coalesce(service_name, '') ASC, coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') ASC, coalesce(md5(body), '') ASC
LIMIT 4"#;

const LIST_TRACE_FILTER_PPL: &str = "source=logs \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | where trace_id = '11111111111111111111111111111111' \
    | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') \
    | sort - `timestamp`, svc, host, body_hash \
    | head 4 \
    | fields service_name, `timestamp`, observed_timestamp, severity_text, body, resource_attributes, scope_attributes, log_attributes, trace_id, span_id, body_hash";

/// `logs_summary_sql` over the one-hour window with 360 buckets (10 s each).
const SUMMARY_SQL: &str = r#"SELECT (arrow_cast(timestamp, 'Int64') - 1700000000000000) * 360 / 3600000000 AS "bucket_index", case when lower(trim(severity_text)) like 'err%' or lower(trim(severity_text)) like 'fatal%' or lower(trim(severity_text)) like 'crit%' or lower(trim(severity_text)) like 'emerg%' or lower(trim(severity_text)) like 'alert%' then 'error' when lower(trim(severity_text)) like 'warn%' then 'warn' when lower(trim(severity_text)) like 'info%' or lower(trim(severity_text)) like 'notice%' then 'info' when lower(trim(severity_text)) like 'debug%' then 'debug' when lower(trim(severity_text)) like 'trace%' then 'trace' else 'info' end AS "level", count(*) AS "n"
FROM "iceberg"."icegate"."logs"
WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000))
GROUP BY (arrow_cast(timestamp, 'Int64') - 1700000000000000) * 360 / 3600000000, case when lower(trim(severity_text)) like 'err%' or lower(trim(severity_text)) like 'fatal%' or lower(trim(severity_text)) like 'crit%' or lower(trim(severity_text)) like 'emerg%' or lower(trim(severity_text)) like 'alert%' then 'error' when lower(trim(severity_text)) like 'warn%' then 'warn' when lower(trim(severity_text)) like 'info%' or lower(trim(severity_text)) like 'notice%' then 'info' when lower(trim(severity_text)) like 'debug%' then 'debug' when lower(trim(severity_text)) like 'trace%' then 'trace' else 'info' end"#;

const SUMMARY_PPL: &str = "source=logs \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | eval severity = lower(trim(severity_text)), bucket_index = timestampdiff(MICROSECOND, timestamp('2023-11-14 22:13:20'), `timestamp`) * 360 / 3600000000, level = case(like(severity, 'err%') or like(severity, 'fatal%') or like(severity, 'crit%') or like(severity, 'emerg%') or like(severity, 'alert%'), 'error', like(severity, 'warn%'), 'warn', like(severity, 'info%') or like(severity, 'notice%'), 'info', like(severity, 'debug%'), 'debug', like(severity, 'trace%'), 'trace' else 'info') \
    | stats count() as n by bucket_index, level";

/// `logs_find_sql` for the locator of `payment failed`.
const FIND_SQL: &str = r#"SELECT "service_name", "timestamp", "observed_timestamp", "severity_text", "body", "resource_attributes", "scope_attributes", "log_attributes", "trace_id", "span_id", coalesce(md5(body), '') AS "body_hash"
FROM "iceberg"."icegate"."logs"
WHERE (timestamp = to_timestamp_micros(1700000020000000)) AND (coalesce(service_name, '') = 'checkout') AND (coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') = 'pod-b') AND (coalesce(md5(body), '') = 'c52e73296e544682fed91beabd0ca8c1')
LIMIT 1"#;

const FIND_PPL: &str = "source=logs \
    | where `timestamp` = timestamp('2023-11-14 22:13:40') \
    | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') \
    | where svc = 'checkout' and host = 'pod-b' and body_hash = 'c52e73296e544682fed91beabd0ca8c1' \
    | head 1 \
    | fields service_name, `timestamp`, observed_timestamp, severity_text, body, resource_attributes, scope_attributes, log_attributes, trace_id, span_id, body_hash";

/// `logs_find_sql` for the locator of B's `tenant b only` line (`md5('tenant b only')` is
/// `e15700efb5d7c3e41adacfac2e2b56c2`).
const FOREIGN_FIND_SQL: &str = r#"SELECT "service_name", "timestamp", "observed_timestamp", "severity_text", "body", "resource_attributes", "scope_attributes", "log_attributes", "trace_id", "span_id", coalesce(md5(body), '') AS "body_hash"
FROM "iceberg"."icegate"."logs"
WHERE (timestamp = to_timestamp_micros(1700000025000000)) AND (coalesce(service_name, '') = 'b-service') AND (coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') = 'pod-z') AND (coalesce(md5(body), '') = 'e15700efb5d7c3e41adacfac2e2b56c2')
LIMIT 1"#;

const FOREIGN_FIND_PPL: &str = "source=logs \
    | where `timestamp` = timestamp('2023-11-14 22:13:45') \
    | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') \
    | where svc = 'b-service' and host = 'pod-z' and body_hash = 'e15700efb5d7c3e41adacfac2e2b56c2' \
    | head 1 \
    | fields service_name, `timestamp`, observed_timestamp, severity_text, body, resource_attributes, scope_attributes, log_attributes, trace_id, span_id, body_hash";

/// `logs_context_sql(locator of payment failed, before = 2, after = 2)`.
const CONTEXT_SQL: &str = r#"SELECT * FROM (SELECT "service_name", "timestamp", "observed_timestamp", "severity_text", "body", "resource_attributes", "scope_attributes", "log_attributes", "trace_id", "span_id", coalesce(md5(body), '') AS "body_hash", 'anchor' AS "context_row_kind"
  FROM "iceberg"."icegate"."logs"
  WHERE (timestamp = to_timestamp_micros(1700000020000000)) AND (coalesce(service_name, '') = 'checkout') AND (coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') = 'pod-b') AND (coalesce(md5(body), '') = 'c52e73296e544682fed91beabd0ca8c1')
  LIMIT 1) AS "context_anchor"
UNION ALL (SELECT "service_name", "timestamp", "observed_timestamp", "severity_text", "body", "resource_attributes", "scope_attributes", "log_attributes", "trace_id", "span_id", coalesce(md5(body), '') AS "body_hash", 'older' AS "context_row_kind"
  FROM "iceberg"."icegate"."logs"
  WHERE (coalesce(service_name, '') = 'checkout') AND (coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') = 'pod-b') AND ((timestamp < to_timestamp_micros(1700000020000000) OR (timestamp = to_timestamp_micros(1700000020000000) AND coalesce(md5(body), '') > 'c52e73296e544682fed91beabd0ca8c1')))
  ORDER BY timestamp DESC, coalesce(md5(body), '') ASC LIMIT 2)
UNION ALL (SELECT "service_name", "timestamp", "observed_timestamp", "severity_text", "body", "resource_attributes", "scope_attributes", "log_attributes", "trace_id", "span_id", coalesce(md5(body), '') AS "body_hash", 'newer' AS "context_row_kind"
  FROM "iceberg"."icegate"."logs"
  WHERE (coalesce(service_name, '') = 'checkout') AND (coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), '') = 'pod-b') AND ((timestamp > to_timestamp_micros(1700000020000000) OR (timestamp = to_timestamp_micros(1700000020000000) AND coalesce(md5(body), '') < 'c52e73296e544682fed91beabd0ca8c1')))
  ORDER BY timestamp ASC, coalesce(md5(body), '') DESC LIMIT 2)"#;

const CONTEXT_PPL: &str = "source=logs \
    | where `timestamp` = timestamp('2023-11-14 22:13:40') \
    | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') \
    | where svc = 'checkout' and host = 'pod-b' and body_hash = 'c52e73296e544682fed91beabd0ca8c1' \
    | head 1 \
    | eval context_row_kind = 'anchor' \
    | fields service_name, `timestamp`, observed_timestamp, severity_text, body, resource_attributes, scope_attributes, log_attributes, trace_id, span_id, body_hash, context_row_kind \
    | append [ source=logs \
        | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') \
        | where svc = 'checkout' and host = 'pod-b' and (`timestamp` < timestamp('2023-11-14 22:13:40') or (`timestamp` = timestamp('2023-11-14 22:13:40') and body_hash > 'c52e73296e544682fed91beabd0ca8c1')) \
        | sort - `timestamp`, body_hash \
        | head 2 \
        | eval context_row_kind = 'older' \
        | fields service_name, `timestamp`, observed_timestamp, severity_text, body, resource_attributes, scope_attributes, log_attributes, trace_id, span_id, body_hash, context_row_kind ] \
    | append [ source=logs \
        | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') \
        | where svc = 'checkout' and host = 'pod-b' and (`timestamp` > timestamp('2023-11-14 22:13:40') or (`timestamp` = timestamp('2023-11-14 22:13:40') and body_hash < 'c52e73296e544682fed91beabd0ca8c1')) \
        | sort `timestamp`, - body_hash \
        | head 2 \
        | eval context_row_kind = 'newer' \
        | fields service_name, `timestamp`, observed_timestamp, severity_text, body, resource_attributes, scope_attributes, log_attributes, trace_id, span_id, body_hash, context_row_kind ]";

/// `facets_sql(window, {service, host})`: `v0` is `service`, `v1` is `host`, in catalog order.
const FACETS_SQL: &str = r#"WITH src AS (SELECT nullif(coalesce(service_name, ''), '') AS "v0", nullif(coalesce(array_element(map_extract(log_attributes, 'host.name'), 1), array_element(map_extract(scope_attributes, 'host.name'), 1), array_element(map_extract(resource_attributes, 'host.name'), 1), ''), '') AS "v1" FROM "iceberg"."icegate"."logs" WHERE (timestamp >= to_timestamp_micros(1700000000000000)) AND (timestamp < to_timestamp_micros(1700003600000000))),
g AS (SELECT v0, v1 FROM src GROUP BY GROUPING SETS ((v0), (v1))),
u AS (SELECT CASE WHEN v0 IS NOT NULL THEN 'service' WHEN v1 IS NOT NULL THEN 'host' END AS field, coalesce(v0, v1) AS value FROM g)
SELECT field, value FROM (SELECT field, value, row_number() OVER (PARTITION BY field ORDER BY value) AS rn FROM u WHERE value IS NOT NULL) t WHERE rn <= 201"#;

const FACETS_PPL: &str = "source=logs \
    | where `timestamp` >= timestamp('2023-11-14 22:13:20') and `timestamp` < timestamp('2023-11-14 23:13:20') \
    | eval v0 = nullif(coalesce(service_name, ''), ''), v1 = nullif(coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), '') \
    | stats values(v0) as v0, values(v1) as v1 \
    | eval v0 = mvindex(v0, 0, 200), v1 = mvindex(v1, 0, 200)";

/// Page one is ordered `timestamp DESC, service, host, body_hash`; the three 22:13:40 lines split
/// on the service (`billing` first) and then on the body hash (`c52e…` before `dd4e…`). The
/// continuation seeks strictly after `retrying`. If tenant B's rows leaked, its identical
/// 22:13:40 line would land on both pages and its 22:13:45 and 22:14:05 lines on the first.
#[tokio::test]
async fn logs_list_first_page_and_continuation_match_their_sql_forms() {
    let (servers, catalog) = ParityServers::start().await;
    seed_logs(&catalog).await;
    let first_page = [
        build_expected_receipt_sent_row(),
        build_expected_cache_miss_row(),
        build_expected_retrying_row(),
        build_expected_payment_failed_row(),
    ];
    let continuation = [
        build_expected_payment_failed_row(),
        build_expected_payment_retried_row(),
        build_expected_order_placed_row(),
        build_expected_cart_opened_row(),
    ];
    assert_rows_in_order(
        "SQL first page",
        &servers.run_sql(TENANT_A, LIST_FIRST_PAGE_SQL).await,
        &first_page,
    );
    assert_rows_in_order(
        "PPL first page",
        &servers.run_ppl(TENANT_A, LIST_FIRST_PAGE_PPL).await,
        &first_page,
    );
    assert_rows_in_order(
        "SQL continuation",
        &servers.run_sql(TENANT_A, LIST_CONTINUATION_SQL).await,
        &continuation,
    );
    assert_rows_in_order(
        "PPL continuation",
        &servers.run_ppl(TENANT_A, LIST_CONTINUATION_PPL).await,
        &continuation,
    );
    servers.shutdown().await;
}

/// Only A's payment line carries the trace id inside A; B's line with the same id stays out.
#[tokio::test]
async fn logs_list_with_a_trace_id_filter_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_logs(&catalog).await;
    let expected = [build_expected_payment_failed_row()];
    assert_rows_in_order(
        "SQL",
        &servers.run_sql(TENANT_A, LIST_TRACE_FILTER_SQL).await,
        &expected,
    );
    assert_rows_in_order(
        "PPL",
        &servers.run_ppl(TENANT_A, LIST_TRACE_FILTER_PPL).await,
        &expected,
    );
    servers.shutdown().await;
}

/// Bucket `i` covers `[22:13:20 + 10 i s, + 10 s)`; the level is the severity prefix, `info`
/// when none matches. The 22:13:19 line is outside the window.
#[tokio::test]
async fn logs_summary_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_logs(&catalog).await;
    let expected = [
        json!({"bucket_index": 0, "level": "info", "n": 1}),
        json!({"bucket_index": 1, "level": "info", "n": 1}),
        json!({"bucket_index": 2, "level": "error", "n": 1}),
        json!({"bucket_index": 2, "level": "info", "n": 1}),
        json!({"bucket_index": 2, "level": "warn", "n": 1}),
        json!({"bucket_index": 3, "level": "debug", "n": 1}),
        json!({"bucket_index": 4, "level": "info", "n": 1}),
    ];
    assert_rows_unordered("SQL", &servers.run_sql(TENANT_A, SUMMARY_SQL).await, &expected);
    assert_rows_unordered("PPL", &servers.run_ppl(TENANT_A, SUMMARY_PPL).await, &expected);
    servers.shutdown().await;
}

/// The locator names exactly one A line. B's identical line projects to the same row, so the
/// tenant scope is pinned by B's own `tenant b only` locator, which names no row as tenant A.
#[tokio::test]
async fn logs_find_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_logs(&catalog).await;
    let expected = [build_expected_payment_failed_row()];
    assert_rows_in_order("SQL", &servers.run_sql(TENANT_A, FIND_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, FIND_PPL).await, &expected);
    assert_rows_in_order(
        "SQL foreign locator",
        &servers.run_sql(TENANT_A, FOREIGN_FIND_SQL).await,
        &[],
    );
    assert_rows_in_order(
        "PPL foreign locator",
        &servers.run_ppl(TENANT_A, FOREIGN_FIND_PPL).await,
        &[],
    );
    servers.shutdown().await;
}

/// The `(checkout, pod-b)` stream of A is 22:13:19, 22:13:25, 22:13:40 (`c52e…`, the anchor),
/// 22:13:40 (`dd4e…`) and 22:14:00. Older walks strictly after the anchor in list order and keeps
/// two (`payment retried`, `cart opened`); newer keeps the one line after it (`receipt sent`).
/// B's `b newer` line at 22:14:05 sits on the same stream and would be a second newer row if it
/// leaked. The branch order of `UNION ALL` is unspecified, so the SQL rows compare unordered;
/// `append` keeps its branches and each branch's sort in order, so the PPL rows compare in order.
#[tokio::test]
async fn logs_context_matches_its_sql_form() {
    let (servers, catalog) = ParityServers::start().await;
    seed_logs(&catalog).await;
    let expected = [
        tag_row_kind(build_expected_payment_failed_row(), "anchor"),
        tag_row_kind(build_expected_payment_retried_row(), "older"),
        tag_row_kind(build_expected_cart_opened_row(), "older"),
        tag_row_kind(build_expected_receipt_sent_row(), "newer"),
    ];
    assert_rows_unordered("SQL", &servers.run_sql(TENANT_A, CONTEXT_SQL).await, &expected);
    assert_rows_in_order("PPL", &servers.run_ppl(TENANT_A, CONTEXT_PPL).await, &expected);
    servers.shutdown().await;
}

/// Services and hosts of A's window; the empty host of `retrying` and the null service of
/// `cache miss` are no value. B's `b-service` and `pod-z` never appear.
#[tokio::test]
async fn log_facets_match_their_sql_forms() {
    let (servers, catalog) = ParityServers::start().await;
    seed_logs(&catalog).await;
    let expected = build_facet_map(&[
        ("host", &["pod-a", "pod-b", "pod-c"][..]),
        ("service", &["billing", "checkout"][..]),
    ]);
    assert_eq!(
        collect_sql_facets(&servers.run_sql(TENANT_A, FACETS_SQL).await),
        expected,
        "SQL"
    );
    let ppl_rows = servers.run_ppl(TENANT_A, FACETS_PPL).await;
    assert_eq!(
        collect_ppl_facets(&ppl_rows, &[("v0", "service"), ("v1", "host")]),
        expected,
        "PPL"
    );
    servers.shutdown().await;
}
