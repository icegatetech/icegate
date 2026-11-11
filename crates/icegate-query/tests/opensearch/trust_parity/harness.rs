//! Flight SQL and the OpenSearch-compatible listener over one runtime, and the canonical row
//! form both answers are compared in.
//!
//! The two listeners share one engine, catalog and WAL store, so a SQL statement and its PPL
//! form read exactly the same rows. Both answers become JSON objects keyed by column: the PPL
//! side as the `jdbc` response states its cells, the SQL side by rendering each Arrow cell the
//! way the `jdbc` response does (timestamps `yyyy-MM-dd HH:mm:ss[.f]` in UTC with trailing
//! zeros dropped, ids as lowercase hex, decimals as doubles, maps as objects keyed by stored
//! key, `NaN` and the infinities as strings).
#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_flight::sql::client::FlightSqlServiceClient;
use chrono::DateTime;
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use futures::TryStreamExt;
use iceberg::Catalog;
use icegate_common::testing::server_task::{DrainOutcome, PORT_BIND_TIMEOUT, SHUTDOWN_TIMEOUT, drain_server_task};
use icegate_common::{ListenerRuntime, TENANT_ID_HEADER};
use icegate_query::flight_sql::FlightSqlConfig;
use icegate_query::opensearch::OpenSearchConfig;
use serde_json::{Map, Number, Value, json};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

use super::tables::create_trust_tables;
use crate::opensearch::harness::{
    PplReply, REQUEST_TIMEOUT, build_in_memory_system_object_store, build_request_client, build_runtime, is_serving,
};

/// The tenant whose rows every parity statement reads.
pub const TENANT_A: &str = "tenant-a";
/// The tenant whose rows are written in the same batch and snapshot as tenant A's and must never
/// be read.
pub const TENANT_B: &str = "tenant-b";
/// `2023-11-14 22:13:20` UTC in microseconds: the start of every search window of this suite.
pub const WINDOW_START_MICROS: i64 = 1_700_000_000_000_000;
/// One second in microseconds.
pub const SECOND_MICROS: i64 = 1_000_000;

/// Tolerance between two double cells: relative to the larger magnitude when that is above 1,
/// absolute below 1. Both forms compute costs in the same operand order, so they agree to the
/// last bits; the tolerance only absorbs the hand-written decimal literals of the expected rows,
/// which are not exactly representable.
const DOUBLE_TOLERANCE: f64 = 1e-9;

/// Flight SQL and the `OpenSearch` listener over one runtime and one temporary warehouse.
pub struct ParityServers {
    /// Client for the `OpenSearch` listener, every request bounded.
    opensearch_client: reqwest::Client,
    /// `http://127.0.0.1:<port>` of the `OpenSearch` listener.
    opensearch_url: String,
    /// A lazily connected channel to the Flight SQL listener.
    flight_channel: Channel,
    /// Cancels both listeners: [`ParityServers::shutdown`] and the `Drop` fire it.
    cancel_token: CancellationToken,
    /// The task serving Flight SQL.
    flight_handle: JoinHandle<()>,
    /// The task serving the `OpenSearch` listener.
    opensearch_handle: JoinHandle<()>,
    /// Held for its `Drop`, which removes the warehouse directory.
    _warehouse: tempfile::TempDir,
}

impl ParityServers {
    /// Start both listeners on port 0 under the `multi` tenant policy, over a catalog holding every
    /// production table and trust's `marks`, `verdicts` and `scans`. WAL reads are on and the WAL
    /// is empty, so every row a test commits is read from Iceberg by both listeners.
    pub async fn start() -> (Self, Arc<dyn Catalog>) {
        let (runtime, catalog, warehouse) = build_runtime().await.expect("the parity runtime");
        // Before either listener starts, so the engine's first session lists trust's tables too.
        create_trust_tables(&catalog).await;
        let cancel_token = CancellationToken::new();

        let flight_config = FlightSqlConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 0,
            ..FlightSqlConfig::default()
        };
        let (flight_port_tx, flight_port_rx) = oneshot::channel::<u16>();
        let flight_runtime = runtime.clone();
        let flight_listener_runtime = ListenerRuntime::plaintext(cancel_token.clone());
        let mut flight_handle = tokio::spawn(async move {
            icegate_query::flight_sql::run_with_port_tx(
                flight_runtime,
                flight_config,
                flight_listener_runtime,
                Some(flight_port_tx),
            )
            .await
            .expect("the Flight SQL listener runs");
        });

        let system_store = build_in_memory_system_object_store().expect("the in-memory system-index store");
        let opensearch_config = OpenSearchConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 0,
            ..OpenSearchConfig::default()
        };
        let (opensearch_port_tx, opensearch_port_rx) = oneshot::channel::<u16>();
        let opensearch_listener_runtime = ListenerRuntime::plaintext(cancel_token.clone());
        let mut opensearch_handle = tokio::spawn(async move {
            icegate_query::opensearch::run_with_port_tx(
                runtime,
                opensearch_config,
                system_store,
                opensearch_listener_runtime,
                Some(opensearch_port_tx),
            )
            .await
            .expect("the OpenSearch listener runs");
        });

        let bound_ports = tokio::time::timeout(PORT_BIND_TIMEOUT, async {
            (flight_port_rx.await, opensearch_port_rx.await)
        })
        .await;
        let Ok((Ok(flight_port), Ok(opensearch_port))) = bound_ports else {
            cancel_token.cancel();
            drain_server_task(&mut flight_handle, "Flight SQL").await;
            drain_server_task(&mut opensearch_handle, "OpenSearch").await;
            panic!("a parity listener reported no bound port");
        };
        let flight_channel = Endpoint::from_shared(format!("http://127.0.0.1:{flight_port}"))
            .expect("Flight SQL endpoint")
            .connect_lazy();
        let servers = Self {
            opensearch_client: build_request_client().expect("reqwest client"),
            opensearch_url: format!("http://127.0.0.1:{opensearch_port}"),
            flight_channel,
            cancel_token,
            flight_handle,
            opensearch_handle,
            _warehouse: warehouse,
        };
        // A panic here drops `servers`, whose `Drop` stops both listeners.
        assert!(
            is_serving(&servers.opensearch_client, &servers.opensearch_url).await,
            "the OpenSearch listener at {} never answered",
            servers.opensearch_url
        );
        (servers, catalog)
    }

    /// Run `sql` over Flight SQL as `tenant` and return every row in the canonical form. A refused
    /// statement fails the test with the server's error and the statement, and so does a statement
    /// whose whole exchange outlasts [`REQUEST_TIMEOUT`], the bound of every `OpenSearch` request.
    pub async fn run_sql(&self, tenant: &str, sql: &str) -> Vec<Value> {
        let batches = tokio::time::timeout(REQUEST_TIMEOUT, self.collect_sql_batches(tenant, sql))
            .await
            .unwrap_or_else(|_| panic!("SQL did not finish within {}s:\n{sql}", REQUEST_TIMEOUT.as_secs()));
        batches.iter().flat_map(render_batch_rows).collect()
    }

    /// Every result batch of `sql` run over Flight SQL as `tenant`, unbounded in time: the caller
    /// bounds the wait.
    async fn collect_sql_batches(&self, tenant: &str, sql: &str) -> Vec<RecordBatch> {
        let mut client = FlightSqlServiceClient::new(self.flight_channel.clone());
        client.set_header(TENANT_ID_HEADER, tenant);
        let flight_info = client
            .execute(sql.to_owned(), None)
            .await
            .unwrap_or_else(|error| panic!("SQL refused ({error}):\n{sql}"));
        let mut batches: Vec<RecordBatch> = Vec::new();
        for endpoint in flight_info.endpoint {
            let ticket = endpoint.ticket.expect("Flight endpoint ticket");
            let stream = client
                .do_get(ticket)
                .await
                .unwrap_or_else(|error| panic!("SQL do_get failed ({error}):\n{sql}"));
            let mut collected: Vec<RecordBatch> = stream
                .try_collect()
                .await
                .unwrap_or_else(|error| panic!("SQL result stream failed ({error}):\n{sql}"));
            batches.append(&mut collected);
        }
        batches
    }

    /// `POST /_plugins/_ppl` with `ppl` as `tenant`; returns the status, the content type and the
    /// body text, whatever the status.
    pub async fn post_ppl(&self, tenant: &str, ppl: &str) -> PplReply {
        let response = self
            .opensearch_client
            .post(format!("{}/_plugins/_ppl", self.opensearch_url))
            .header(TENANT_ID_HEADER, tenant)
            .header("content-type", "application/json")
            .body(json!({ "query": ppl }).to_string())
            .send()
            .await
            .expect("PPL request");
        PplReply::from_response(response).await
    }

    /// Run `ppl` as `tenant` and return every `jdbc` row as an object keyed by its `schema` names.
    /// Anything but a `200` fails the test with the body and the query.
    pub async fn run_ppl(&self, tenant: &str, ppl: &str) -> Vec<Value> {
        let reply = self.post_ppl(tenant, ppl).await;
        assert_eq!(
            reply.status, 200,
            "PPL refused ({}): {}\nquery: {ppl}",
            reply.content_type, reply.text
        );
        let body = reply.parse_json_body();
        let names: Vec<String> = body["schema"]
            .as_array()
            .expect("jdbc schema")
            .iter()
            .map(|column| column["name"].as_str().expect("column name").to_owned())
            .collect();
        body["datarows"]
            .as_array()
            .expect("jdbc datarows")
            .iter()
            .map(|row| {
                let cells = row.as_array().expect("jdbc row");
                assert_eq!(cells.len(), names.len(), "a jdbc row has one cell per schema column");
                Value::Object(names.iter().cloned().zip(cells.iter().cloned()).collect())
            })
            .collect()
    }

    /// Stop both listeners. A panic or a hang of either server task fails the test.
    pub async fn shutdown(mut self) {
        self.cancel_token.cancel();
        for (handle, name) in [
            (&mut self.flight_handle, "Flight SQL"),
            (&mut self.opensearch_handle, "OpenSearch"),
        ] {
            let outcome = drain_server_task(handle, name).await;
            assert_eq!(
                outcome,
                DrainOutcome::Finished,
                "the {name} listener did not stop within {}s",
                SHUTDOWN_TIMEOUT.as_secs()
            );
        }
    }
}

impl Drop for ParityServers {
    /// A test that panics before [`ParityServers::shutdown`] still stops both listeners.
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.flight_handle.abort();
        self.opensearch_handle.abort();
    }
}

/// `micros` since the epoch as the `jdbc` response renders a timestamp: UTC, no `T`, no zone,
/// 1–6 fraction digits with trailing zeros dropped, none when the fraction is zero.
pub fn render_timestamp(micros: i64) -> String {
    let instant = DateTime::from_timestamp_micros(micros).expect("an instant chrono represents");
    let whole_seconds_text = instant.format("%Y-%m-%d %H:%M:%S").to_string();
    let fraction = micros.rem_euclid(SECOND_MICROS);
    if fraction == 0 {
        return whole_seconds_text;
    }
    let digits = format!("{fraction:06}");
    format!("{whole_seconds_text}.{}", digits.trim_end_matches('0'))
}

/// Every row of `batch` as an object keyed by column name, each cell in the canonical form.
fn render_batch_rows(batch: &RecordBatch) -> Vec<Value> {
    let schema = batch.schema();
    (0..batch.num_rows())
        .map(|row| {
            let object: Map<String, Value> = schema
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(field, column)| (field.name().clone(), render_canonical_cell(column.as_ref(), row)))
                .collect();
            Value::Object(object)
        })
        .collect()
}

/// A double as the `jdbc` response states it: a number, or `"NaN"`, `"Infinity"`, `"-Infinity"`.
fn render_canonical_double(value: f64) -> Value {
    if value.is_nan() {
        json!("NaN")
    } else if value.is_infinite() {
        json!(if value > 0.0 { "Infinity" } else { "-Infinity" })
    } else {
        Number::from_f64(value).map_or(Value::Null, Value::Number)
    }
}

/// A timestamp cell of `unit` in microseconds since the epoch.
fn read_timestamp_micros(column: &dyn Array, unit: TimeUnit, row: usize) -> i64 {
    match unit {
        TimeUnit::Second => column.as_primitive::<TimestampSecondType>().value(row) * SECOND_MICROS,
        TimeUnit::Millisecond => column.as_primitive::<TimestampMillisecondType>().value(row) * 1_000,
        TimeUnit::Microsecond => column.as_primitive::<TimestampMicrosecondType>().value(row),
        TimeUnit::Nanosecond => column.as_primitive::<TimestampNanosecondType>().value(row).div_euclid(1_000),
    }
}

/// A `MAP` cell as an object keyed by its stored keys.
fn render_canonical_map(column: &dyn Array, row: usize) -> Value {
    let entries = column.as_map().value(row);
    let (keys, values) = (entries.column(0), entries.column(1));
    let object: Map<String, Value> = (0..entries.len())
        .map(|index| {
            let Value::String(key) = render_canonical_cell(keys.as_ref(), index) else {
                panic!("a MAP key is a string");
            };
            (key, render_canonical_cell(values.as_ref(), index))
        })
        .collect();
    Value::Object(object)
}

/// One Arrow cell in the canonical form. Panics on a type no parity statement returns.
fn render_canonical_cell(column: &dyn Array, row: usize) -> Value {
    if column.is_null(row) {
        return Value::Null;
    }
    match column.data_type() {
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        DataType::Boolean => json!(column.as_boolean().value(row)),
        DataType::Int8 => json!(column.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => json!(column.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => json!(column.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => json!(column.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => json!(column.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => json!(column.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => render_canonical_double(f64::from(column.as_primitive::<Float32Type>().value(row))),
        DataType::Float64 => render_canonical_double(column.as_primitive::<Float64Type>().value(row)),
        // Decimals are read as doubles on the PPL side (decimal columns read as `double`).
        DataType::Decimal128(_, _) => render_canonical_double(
            column
                .as_primitive::<Decimal128Type>()
                .value_as_string(row)
                .parse::<f64>()
                .expect("decimal text parses as a double"),
        ),
        DataType::Timestamp(unit, _) => json!(render_timestamp(read_timestamp_micros(column, *unit, row))),
        DataType::FixedSizeBinary(_) => json!(hex::encode(column.as_fixed_size_binary().value(row))),
        DataType::Binary => json!(hex::encode(column.as_binary::<i32>().value(row))),
        DataType::Map(_, _) => render_canonical_map(column, row),
        DataType::List(_) => {
            let items = column.as_list::<i32>().value(row);
            Value::Array(
                (0..items.len())
                    .map(|index| render_canonical_cell(items.as_ref(), index))
                    .collect(),
            )
        }
        DataType::Struct(_) => {
            let fields = column.as_struct();
            let object: Map<String, Value> = fields
                .fields()
                .iter()
                .zip(fields.columns())
                .map(|(field, child)| (field.name().clone(), render_canonical_cell(child.as_ref(), row)))
                .collect();
            Value::Object(object)
        }
        other => panic!("no canonical form for a {other} cell"),
    }
}

/// Whether two numbers are equal and of one kind. An integer matches only an equal integer and a
/// double only a double within [`DOUBLE_TOLERANCE`], so a form answering a column with another
/// type than its counterpart (`2` against `2.0`) fails even when the values agree.
fn is_number_matching(actual: &Number, expected: &Number) -> bool {
    match (actual.as_f64(), expected.as_f64()) {
        (Some(actual_double), Some(expected_double)) if actual.is_f64() && expected.is_f64() => {
            (actual_double - expected_double).abs()
                <= DOUBLE_TOLERANCE * actual_double.abs().max(expected_double.abs()).max(1.0)
        }
        // `Number`'s equality compares integers by value and never equates an integer with a
        // double, so this arm also refuses a mixed pair.
        _ => actual == expected,
    }
}

/// Whether `actual` equals `expected`, with numbers compared by [`is_number_matching`] and objects
/// compared as maps (same keys, matching values).
fn is_matching(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Number(actual), Value::Number(expected)) => is_number_matching(actual, expected),
        (Value::Array(actual), Value::Array(expected)) => {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected)
                    .all(|(actual_item, expected_item)| is_matching(actual_item, expected_item))
        }
        (Value::Object(actual), Value::Object(expected)) => {
            actual.len() == expected.len()
                && expected
                    .iter()
                    .all(|(key, value)| actual.get(key).is_some_and(|cell| is_matching(cell, value)))
        }
        _ => actual == expected,
    }
}

/// Assert that `actual` holds exactly the `expected` rows in the same order. `form` names the
/// statement form in the failure message (`"SQL"`, `"PPL"`).
pub fn assert_rows_in_order(form: &str, actual: &[Value], expected: &[Value]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{form}: {} rows, expected {}; first rows: {:#?}",
        actual.len(),
        expected.len(),
        actual.iter().take(5).collect::<Vec<_>>()
    );
    for (index, (row, expected_row)) in actual.iter().zip(expected).enumerate() {
        assert!(
            is_matching(row, expected_row),
            "{form}: row {index} is\n{row:#}\nexpected\n{expected_row:#}"
        );
    }
}

/// Assert that `actual` holds exactly the `expected` rows in any order: each expected row is
/// matched against a distinct actual row.
pub fn assert_rows_unordered(form: &str, actual: &[Value], expected: &[Value]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{form}: {} rows, expected {}:\n{actual:#?}",
        actual.len(),
        expected.len()
    );
    let mut unmatched_rows: Vec<&Value> = actual.iter().collect();
    for expected_row in expected {
        let position = unmatched_rows
            .iter()
            .position(|row| is_matching(row, expected_row))
            .unwrap_or_else(|| {
                panic!("{form}: no row matches\n{expected_row:#}\nunmatched rows:\n{unmatched_rows:#?}")
            });
        unmatched_rows.swap_remove(position);
    }
}

/// Facet values by field label, as stated by a test: each field's values ascending.
pub fn build_facet_map(fields: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    fields
        .iter()
        .map(|(label, values)| {
            (
                (*label).to_owned(),
                values.iter().map(|value| (*value).to_owned()).collect(),
            )
        })
        .collect()
}

/// The facets of the SQL form: its `(field, value)` rows grouped by `field`, values ascending in
/// byte order (the SQL result carries no order).
pub fn collect_sql_facets(rows: &[Value]) -> BTreeMap<String, Vec<String>> {
    let mut facets: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let field = row["field"].as_str().expect("facet field label").to_owned();
        let value = row["value"].as_str().expect("facet value").to_owned();
        facets.entry(field).or_default().push(value);
    }
    for values in facets.values_mut() {
        values.sort_unstable();
    }
    facets
}

/// The facets of the PPL form: its one row of `values()` arrays, each column relabelled by
/// `labels` (`(column, label)`), in the order the response holds them. A field with no value
/// (a `null` cell or an empty array) is absent, as the SQL form returns no row for it. Panics
/// when a labelled column is missing from the row or holds anything but an array of strings, so
/// a misspelled label or a dropped column never reads as no value.
pub fn collect_ppl_facets(rows: &[Value], labels: &[(&str, &str)]) -> BTreeMap<String, Vec<String>> {
    let [row] = rows else {
        panic!("the PPL facets form answers one row, got {rows:#?}");
    };
    labels
        .iter()
        .filter_map(|(column, label)| {
            let cell = row
                .get(*column)
                .unwrap_or_else(|| panic!("the PPL facets row has no `{column}` column: {row:#}"));
            let values: Vec<String> = match cell {
                Value::Null => Vec::new(),
                Value::Array(items) => items
                    .iter()
                    .map(|item| item.as_str().expect("facet value").to_owned())
                    .collect(),
                other => panic!("the PPL facets column `{column}` is not a values() array: {other}"),
            };
            (!values.is_empty()).then(|| ((*label).to_owned(), values))
        })
        .collect()
}
