//! The PPL commands trust needs beyond the core: `join` and subsearches, over real HTTP. Every row is
//! written here with explicit values, and every expected row is stated by hand from the rules in
//! `ppl/README.md`.
#![allow(clippy::expect_used)]

use icegate_common::schema::{COL_NAME, COL_SERVICE_NAME, COL_TIMESTAMP};
use icegate_common::{LOGS_TABLE, SPANS_TABLE};
use serde_json::{Value, json};

use super::data::{Cell, Row, RowWriteError, commit_rows, write_wal_segment};
use super::harness::{PplReply, TestServer};

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";
const PPL_ERROR_CONTENT_TYPE: &str = "text/plain; charset=UTF-8";
/// A system index a test creates; no join side may read it.
const SYSTEM_INDEX: &str = ".kibana_1";
/// 2026-10-08T12:00:00Z in microseconds, computed outside the code.
const NOON: i64 = 1_791_460_800_000_000;
const SECOND: i64 = 1_000_000;

/// Runs `query` as `tenant` and returns the jdbc answer; any status but 200 fails the test.
async fn run_ppl(server: &TestServer, tenant: &str, query: &str) -> Value {
    let reply = server.post_ppl(tenant, "/_plugins/_ppl", &json!({ "query": query })).await;
    assert_eq!(reply.status, 200, "{query}: {}", reply.text);
    reply.parse_json_body()
}

/// Runs `query` as `tenant`, expects the PPL envelope's 400 and returns the reply.
async fn refuse_ppl(server: &TestServer, tenant: &str, query: &str) -> PplReply {
    let reply = server.post_ppl(tenant, "/_plugins/_ppl", &json!({ "query": query })).await;
    assert_eq!(reply.status, 400, "{query}: {}", reply.text);
    assert_eq!(reply.content_type, PPL_ERROR_CONTENT_TYPE, "{query}");
    reply
}

/// The logical and physical plan text `_explain` answers for `query`.
async fn explain_ppl(server: &TestServer, tenant: &str, query: &str) -> (String, String) {
    let reply = server
        .post_ppl(tenant, "/_plugins/_ppl/_explain", &json!({ "query": query }))
        .await;
    assert_eq!(reply.status, 200, "{query}: {}", reply.text);
    let body = reply.parse_json_body();
    let plan_text = |key: &str| {
        body["calcite"][key]
            .as_str()
            .unwrap_or_else(|| panic!("no calcite.{key} in {body}"))
            .to_string()
    };
    (plan_text("logical"), plan_text("physical"))
}

/// Asserts that `error`, the `error` object of a PPL envelope, is the semantic refusal.
fn assert_semantic_error(error: &Value) {
    assert_eq!(error["type"], "SemanticCheckException", "{error}");
    assert_eq!(error["code"], "SEMANTIC_ERROR", "{error}");
}

/// Asserts that `reply` is the semantic refusal and that its reason mentions `expected_text`.
fn assert_semantic_refusal(reply: &PplReply, expected_text: &str) {
    let error = &reply.parse_json_body()["error"];
    assert_semantic_error(error);
    assert!(
        error["reason"].as_str().is_some_and(|reason| reason.contains(expected_text)),
        "{}",
        reply.text
    );
}

/// The lines of `plan` that mention `operator`.
fn find_plan_lines<'p>(plan: &'p str, operator: &str) -> Vec<&'p str> {
    plan.lines().filter(|line| line.contains(operator)).collect()
}

/// One entry of a jdbc answer's `schema`.
fn build_schema_column(name: &str, column_type: &str) -> Value {
    json!({ "name": name, "type": column_type })
}

/// Creates [`SYSTEM_INDEX`] for `tenant` through the index API; any status but 200 fails the test.
///
/// # Errors
///
/// The transport error when the request cannot be sent.
async fn create_system_index(server: &TestServer, tenant: &str) -> Result<(), reqwest::Error> {
    let created = server
        .request(reqwest::Method::PUT, Some(tenant), &format!("/{SYSTEM_INDEX}"))
        .header("content-type", "application/json")
        .body(json!({ "mappings": { "properties": { "x": { "type": "keyword" } } } }).to_string())
        .send()
        .await?;
    assert_eq!(created.status().as_u16(), 200, "PUT /{SYSTEM_INDEX}");
    Ok(())
}

/// Rows on each side (Iceberg, WAL) of the order tests: more than the engine's batch size, so each side
/// arrives in more than one record batch.
const ROWS_PER_SIDE: usize = 9_000;

/// The bodies `00000`, `00001`, … of `count` numbered rows: row `k`'s body is `k` in five digits.
fn build_numbered_bodies(count: usize) -> Vec<String> {
    (0..count).map(|offset| format!("{offset:05}")).collect()
}

/// Tenant-A log rows at `NOON + k` µs with body `bodies[k]`, for every `k` of the given parity: parity 0 for the
/// even rows, 1 for the odd ones. Split by parity across Iceberg and the WAL, the two sides interleave in time,
/// so only an explicit order can put them in sequence.
fn build_numbered_rows(bodies: &[String], parity: usize) -> Vec<Row<'_>> {
    bodies
        .iter()
        .zip(0_i64..)
        .skip(parity)
        .step_by(2)
        .map(|(body, offset)| {
            build_log_row(
                TENANT_A,
                NOON + offset,
                vec![(icegate_common::schema::COL_BODY, Cell::Text(body))],
            )
        })
        .collect()
}

/// A log row of `tenant` at `micros` with `cells`.
fn build_log_row<'a>(tenant: &'a str, micros: i64, mut cells: Vec<(&'a str, Cell<'a>)>) -> Row<'a> {
    cells.push((COL_TIMESTAMP, Cell::Micros(micros)));
    Row { tenant, cells }
}

/// A span row of `tenant` at `micros` named `name` in `service_name`; ids keep their zero default.
fn build_span_row<'a>(tenant: &'a str, micros: i64, service_name: &'a str, name: &'a str) -> Row<'a> {
    Row {
        tenant,
        cells: vec![
            (COL_TIMESTAMP, Cell::Micros(micros)),
            (COL_SERVICE_NAME, Cell::Text(service_name)),
            (COL_NAME, Cell::Text(name)),
        ],
    }
}

/// `join`: hash keys from the equalities, the residual kept in the join, aliases, tenant scope, no cap.
mod join {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, Decimal128Array, FixedSizeBinaryArray, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use icegate_common::schema::{
        COL_BODY, COL_CURRENCY, COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_INPUT_USD_PER_1M,
        COL_MIN_INPUT_TOKENS, COL_MODEL, COL_OUTPUT_USD_PER_1M, COL_PARENT_SPAN_ID, COL_PROVIDER, COL_REGION,
        COL_SERVICE_TIER, COL_SOURCE, COL_SPAN_ID, COL_TENANT_ID, COL_TRACE_ID, COL_VALID_FROM, COL_VALID_FROM_SOURCE,
        PRICE_DECIMAL_PRECISION, PRICE_DECIMAL_SCALE,
    };
    use icegate_common::{OPERATIONS_TABLE, PRICES_TABLE};

    use super::super::data::commit_table_columns;
    use super::super::test_rows::OPERATION_NAME;
    use super::*;

    /// `operations` columns that have no shared constant in `icegate_common::schema`.
    const PROVIDER_NAME: &str = "provider_name";
    const REQUEST_MODEL: &str = "request_model";
    const INPUT_TOKENS: &str = "input_tokens";
    const OUTPUT_TOKENS: &str = "output_tokens";

    /// 2026-01-01, 2026-03-01, 2026-06-01 and 2026-07-01 at 00:00 UTC in microseconds, computed outside
    /// the code.
    const JANUARY_1: i64 = 1_767_225_600_000_000;
    const MARCH_1: i64 = 1_772_323_200_000_000;
    const JUNE_1: i64 = 1_780_272_000_000_000;
    const JULY_1: i64 = 1_782_864_000_000_000;

    /// The parent span id of the child span in the residual test.
    const PARENT_SPAN_ID: [u8; 8] = [0x01, 0, 0, 0, 0, 0, 0, 0];

    fn build_string_column(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn build_long_column(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn build_micros_column(values: &[i64]) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(values.to_vec()))
    }

    /// A price column holding `tenths` / 10 USD per row, at the schema's decimal precision and scale.
    fn build_rate_column(tenths: &[i128]) -> Result<ArrayRef, RowWriteError> {
        let scale = i8::try_from(PRICE_DECIMAL_SCALE)?;
        let precision = u8::try_from(PRICE_DECIMAL_PRECISION)?;
        let unit = 10_i128.pow(PRICE_DECIMAL_SCALE - 1);
        Ok(Arc::new(
            Decimal128Array::from(tenths.iter().map(|value| value * unit).collect::<Vec<_>>())
                .with_precision_and_scale(precision, scale)?,
        ))
    }

    fn build_id_column<const N: usize>(values: &[[u8; N]]) -> Result<ArrayRef, RowWriteError> {
        Ok(Arc::new(FixedSizeBinaryArray::try_from_iter(values.iter())?))
    }

    /// Tenant-A logs `l1`–`l3` and spans `s1`–`s3`, plus a tenant-B span whose service only `l3` shares.
    async fn commit_equality_rows(catalog: &Arc<dyn iceberg::Catalog>) -> Result<(), RowWriteError> {
        commit_rows(
            catalog,
            LOGS_TABLE,
            &[
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("l1")), (COL_SERVICE_NAME, Cell::Text("checkout"))],
                ),
                build_log_row(
                    TENANT_A,
                    NOON + SECOND,
                    vec![(COL_BODY, Cell::Text("l2")), (COL_SERVICE_NAME, Cell::Text("cart"))],
                ),
                build_log_row(
                    TENANT_A,
                    NOON + 2 * SECOND,
                    vec![(COL_BODY, Cell::Text("l3")), (COL_SERVICE_NAME, Cell::Text("search"))],
                ),
            ],
        )
        .await?;
        commit_rows(
            catalog,
            SPANS_TABLE,
            &[
                build_span_row(TENANT_A, NOON, "checkout", "s1"),
                build_span_row(TENANT_A, NOON, "checkout", "s2"),
                build_span_row(TENANT_A, NOON, "cart", "s3"),
                build_span_row(TENANT_B, NOON, "search", "b-span"),
            ],
        )
        .await
    }

    fn format_equality_join(kind: &str) -> String {
        format!(
            "source=logs | fields body, service_name | {kind} join left=l right=s on l.service_name = s.service_name \
             [ source=spans | fields name, service_name ] | sort body, name"
        )
    }

    #[tokio::test]
    async fn left_and_inner_joins_on_equality_answer_the_stated_rows() {
        let (server, catalog) = TestServer::start().await;
        commit_equality_rows(&catalog).await.expect("commit the equality rows");

        let left_answer = run_ppl(&server, TENANT_A, &format_equality_join("left")).await;
        assert_eq!(
            left_answer["schema"],
            json!([
                build_schema_column("body", "string"),
                build_schema_column("service_name", "string"),
                build_schema_column("name", "string"),
                build_schema_column("s.service_name", "string")
            ])
        );
        assert_eq!(
            left_answer["datarows"],
            json!([
                ["l1", "checkout", "s1", "checkout"],
                ["l1", "checkout", "s2", "checkout"],
                ["l2", "cart", "s3", "cart"],
                ["l3", "search", null, null]
            ])
        );

        let inner_answer = run_ppl(&server, TENANT_A, &format_equality_join("inner")).await;
        assert_eq!(
            inner_answer["datarows"],
            json!([
                ["l1", "checkout", "s1", "checkout"],
                ["l1", "checkout", "s2", "checkout"],
                ["l2", "cart", "s3", "cart"]
            ])
        );
        server.shutdown().await;
    }

    /// `l1` matches `s1` by time; `l2` precedes `s1` and `s1` has a parent, so the residual fails and the
    /// left join keeps `l2` with NULLs; `l3` follows no span in time but `s2` is a root, so `isnull` matches.
    const RESIDUAL_JOIN: &str = "source=logs | fields body, service_name, timestamp \
         | left join left=l right=s on l.service_name = s.service_name \
           and (l.timestamp >= s.timestamp or isnull(s.parent_span_id)) \
           [ source=spans | fields name, service_name, timestamp, parent_span_id ] \
         | fields body, name | sort body";

    #[tokio::test]
    async fn a_left_join_with_or_ranges_and_isnull_keeps_unmatched_left_rows() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[
                build_log_row(
                    TENANT_A,
                    NOON + 10 * SECOND,
                    vec![(COL_BODY, Cell::Text("l1")), (COL_SERVICE_NAME, Cell::Text("checkout"))],
                ),
                build_log_row(
                    TENANT_A,
                    NOON - 10 * SECOND,
                    vec![(COL_BODY, Cell::Text("l2")), (COL_SERVICE_NAME, Cell::Text("checkout"))],
                ),
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("l3")), (COL_SERVICE_NAME, Cell::Text("cart"))],
                ),
            ],
        )
        .await
        .expect("commit logs");
        let mut child = build_span_row(TENANT_A, NOON, "checkout", "s1");
        child.cells.push((COL_PARENT_SPAN_ID, Cell::Bytes(&PARENT_SPAN_ID)));
        commit_rows(
            &catalog,
            SPANS_TABLE,
            &[child, build_span_row(TENANT_A, NOON + 100 * SECOND, "cart", "s2")],
        )
        .await
        .expect("commit spans");

        let answer = run_ppl(&server, TENANT_A, RESIDUAL_JOIN).await;
        assert_eq!(answer["datarows"], json!([["l1", "s1"], ["l2", null], ["l3", "s2"]]));
        server.shutdown().await;
    }

    #[tokio::test]
    async fn aliases_qualify_columns_on_both_sides() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("x1")), (COL_SERVICE_NAME, Cell::Text("checkout"))],
                ),
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("x2")), (COL_SERVICE_NAME, Cell::Text("checkout"))],
                ),
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("x3")), (COL_SERVICE_NAME, Cell::Text("cart"))],
                ),
            ],
        )
        .await
        .expect("commit logs");
        // Committed before the first query: the engine reads a table snapshot it refreshes on an interval.
        commit_rows(
            &catalog,
            SPANS_TABLE,
            &[build_span_row(TENANT_A, NOON, "cart", "s-cart")],
        )
        .await
        .expect("commit spans");

        let self_join_query = "source=logs | fields body, service_name \
             | inner join left=x right=y on x.service_name = y.service_name and x.body < y.body \
               [ source=logs | fields body, service_name ] | sort body";
        let answer = run_ppl(&server, TENANT_A, self_join_query).await;
        assert_eq!(
            answer["schema"],
            json!([
                build_schema_column("body", "string"),
                build_schema_column("service_name", "string"),
                build_schema_column("y.body", "string"),
                build_schema_column("y.service_name", "string")
            ])
        );
        assert_eq!(answer["datarows"], json!([["x1", "checkout", "x2", "checkout"]]));

        // A table name qualifies too while the other side does not offer the same name.
        let table_qualified_query = "source=logs | fields body, service_name | where body = 'x3' \
             | inner join on logs.service_name = spans.service_name [ source=spans | fields name, service_name ]";
        let answer = run_ppl(&server, TENANT_A, table_qualified_query).await;
        assert_eq!(
            answer["schema"],
            json!([
                build_schema_column("body", "string"),
                build_schema_column("service_name", "string"),
                build_schema_column("name", "string"),
                build_schema_column("spans.service_name", "string")
            ])
        );
        assert_eq!(answer["datarows"], json!([["x3", "cart", "s-cart", "cart"]]));

        // `body` is offered by both sides, so the bare name is not visible.
        let ambiguous_reply = refuse_ppl(
            &server,
            TENANT_A,
            "source=logs | fields body | inner join right=y on body = y.body [ source=logs | fields body ]",
        )
        .await;
        let ambiguous_error = &ambiguous_reply.parse_json_body()["error"];
        assert_eq!(
            ambiguous_error["type"], "SemanticCheckException",
            "{}",
            ambiguous_reply.text
        );
        assert_eq!(ambiguous_error["code"], "FIELD_NOT_FOUND", "{}", ambiguous_reply.text);
        assert_eq!(ambiguous_error["context"]["field"], "body", "{}", ambiguous_reply.text);

        // A right side over two indices carries no table qualifier, so its colliding `service_name` has
        // nothing to be renamed by until `right=` names one.
        let unrenamable_reply = refuse_ppl(
            &server,
            TENANT_A,
            "source=logs | fields body, service_name | inner join on body = name \
             [ source=logs,spans | fields name, service_name ]",
        )
        .await;
        assert_semantic_error(&unrenamable_reply.parse_json_body()["error"]);
        let renamed_query = "source=logs | fields body, service_name \
             | inner join left=l right=r on l.service_name = r.service_name \
               [ source=logs,spans | fields name, service_name ] | sort body, name";
        let renamed_answer = run_ppl(&server, TENANT_A, renamed_query).await;
        assert_eq!(
            renamed_answer["schema"],
            json!([
                build_schema_column("body", "string"),
                build_schema_column("service_name", "string"),
                build_schema_column("name", "string"),
                build_schema_column("r.service_name", "string")
            ])
        );
        // Each log meets the two logs and, for `cart`, the span of its service; a log row has no `name`.
        assert_eq!(
            renamed_answer["datarows"],
            json!([
                ["x1", "checkout", null, "checkout"],
                ["x1", "checkout", null, "checkout"],
                ["x2", "checkout", null, "checkout"],
                ["x2", "checkout", null, "checkout"],
                ["x3", "cart", null, "cart"],
                ["x3", "cart", "s-cart", "cart"]
            ])
        );

        // A chained join reusing `right=s` would rename its `service_name` onto the `s.service_name` the
        // first join already output; a different alias renames it apart.
        let first_join = "source=logs | fields body, service_name \
             | inner join left=l right=s on l.service_name = s.service_name \
             [ source=spans | fields name, service_name ]";
        let reused_alias_reply = refuse_ppl(
            &server,
            TENANT_A,
            &format!(
                "{first_join} | inner join left=l right=s on l.service_name = s.service_name \
                 [ source=spans | fields name, service_name ]"
            ),
        )
        .await;
        let reused_alias_error = &reused_alias_reply.parse_json_body()["error"];
        assert_semantic_error(reused_alias_error);
        assert!(
            reused_alias_error["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("two columns `s.service_name`") && reason.contains("right=")),
            "{}",
            reused_alias_reply.text
        );
        let chained_answer = run_ppl(
            &server,
            TENANT_A,
            &format!(
                "{first_join} | inner join left=l right=t on l.service_name = t.service_name \
                 [ source=spans | fields name, service_name ]"
            ),
        )
        .await;
        assert_eq!(
            chained_answer["schema"],
            json!([
                build_schema_column("body", "string"),
                build_schema_column("service_name", "string"),
                build_schema_column("name", "string"),
                build_schema_column("s.service_name", "string"),
                build_schema_column("t.name", "string"),
                build_schema_column("t.service_name", "string")
            ])
        );
        assert_eq!(
            chained_answer["datarows"],
            json!([["x3", "cart", "s-cart", "cart", "s-cart", "cart"]])
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn max_zero_is_unlimited() {
        let (server, catalog) = TestServer::start().await;
        commit_equality_rows(&catalog).await.expect("commit the equality rows");
        for options in ["max=0", ""] {
            let query = format!(
                "source=logs | where body = 'l1' | fields body, service_name \
                 | left join {options} left=l right=s on l.service_name = s.service_name \
                   [ source=spans | fields name, service_name ] | fields body, name | sort name"
            );
            assert_eq!(
                run_ppl(&server, TENANT_A, &query).await["datarows"],
                json!([["l1", "s1"], ["l1", "s2"]]),
                "{query}"
            );
        }
        let capped_reply = refuse_ppl(
            &server,
            TENANT_A,
            "source=logs | left join max=1 left=l right=s on l.service_name = s.service_name [ source=spans ]",
        )
        .await;
        let capped_error = &capped_reply.parse_json_body()["error"];
        assert_semantic_error(capped_error);
        assert!(
            capped_error["reason"].as_str().is_some_and(|reason| reason.contains("max=1")),
            "{}",
            capped_reply.text
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn the_join_plan_hashes_on_the_equality_conjuncts_with_a_residual_filter() {
        let (server, _catalog) = TestServer::start().await;
        let (logical_plan, physical_plan) = explain_ppl(&server, TENANT_A, RESIDUAL_JOIN).await;

        let logical_joins = find_plan_lines(&logical_plan, "Left Join:");
        assert_eq!(logical_joins.len(), 1, "{logical_plan}");
        assert!(
            logical_joins[0].contains("Filter:"),
            "the residual stays in the join:\n{logical_plan}"
        );

        let hash_joins = find_plan_lines(&physical_plan, "HashJoinExec");
        assert_eq!(hash_joins.len(), 1, "{physical_plan}");
        let hash_join = hash_joins[0];
        assert!(
            !hash_join.contains("on=[]"),
            "the equality is a hash key:\n{physical_plan}"
        );
        let residual = hash_join.split_once("filter=").map_or("", |(_, rest)| rest);
        assert!(
            residual.contains(" OR "),
            "the residual is the join filter:\n{physical_plan}"
        );
        assert!(
            find_plan_lines(&physical_plan, "NestedLoopJoinExec").is_empty(),
            "{physical_plan}"
        );
        assert!(
            find_plan_lines(&physical_plan, "FilterExec")
                .iter()
                .all(|line| !line.contains("IS NULL")),
            "no post-join filter carries the residual:\n{physical_plan}"
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn a_priced_block_style_left_join_against_prices_effective_answers_the_stated_costs() {
        let (server, catalog) = TestServer::start().await;
        // Key (openai, gpt-4o, standard, global, 0): January then June, so the January row's valid_to is
        // June 1. Key (…, 2000) is the long-prompt tier from January. The `batch` row is filtered out by the
        // right side's own `where`.
        commit_table_columns(
            &catalog,
            PRICES_TABLE,
            4,
            &[
                (
                    COL_PROVIDER,
                    build_string_column(&["openai", "openai", "openai", "openai"]),
                ),
                (
                    COL_MODEL,
                    build_string_column(&["gpt-4o", "gpt-4o", "gpt-4o", "gpt-4o"]),
                ),
                (
                    COL_SERVICE_TIER,
                    build_string_column(&["standard", "standard", "standard", "batch"]),
                ),
                (
                    COL_REGION,
                    build_string_column(&["global", "global", "global", "global"]),
                ),
                (COL_MIN_INPUT_TOKENS, build_long_column(&[0, 0, 2000, 0])),
                (
                    COL_VALID_FROM,
                    build_micros_column(&[JANUARY_1, JUNE_1, JANUARY_1, JANUARY_1]),
                ),
                (
                    COL_VALID_FROM_SOURCE,
                    build_string_column(&["vendor", "vendor", "vendor", "vendor"]),
                ),
                (
                    COL_INPUT_USD_PER_1M,
                    build_rate_column(&[25, 30, 20, 10]).expect("rate column"),
                ),
                (
                    COL_OUTPUT_USD_PER_1M,
                    build_rate_column(&[100, 150, 80, 10]).expect("rate column"),
                ),
                (COL_CURRENCY, build_string_column(&["USD", "USD", "USD", "USD"])),
                (COL_SOURCE, build_string_column(&["test", "test", "test", "test"])),
            ],
        )
        .await
        .expect("commit prices");
        commit_table_columns(
            &catalog,
            OPERATIONS_TABLE,
            5,
            &[
                (
                    COL_TENANT_ID,
                    build_string_column(&[TENANT_A, TENANT_A, TENANT_A, TENANT_A, TENANT_B]),
                ),
                (
                    COL_TRACE_ID,
                    build_id_column(&[[0x0a; 16], [0x0a; 16], [0x0a; 16], [0x0a; 16], [0x0b; 16]]).expect("id column"),
                ),
                (
                    COL_SPAN_ID,
                    build_id_column(&[[0x01; 8], [0x02; 8], [0x03; 8], [0x04; 8], [0x05; 8]]).expect("id column"),
                ),
                (
                    COL_TIMESTAMP,
                    build_micros_column(&[MARCH_1, JULY_1, MARCH_1, MARCH_1, MARCH_1]),
                ),
                (
                    COL_END_TIMESTAMP,
                    build_micros_column(&[MARCH_1, JULY_1, MARCH_1, MARCH_1, MARCH_1]),
                ),
                (COL_DURATION_MICROS, build_long_column(&[0, 0, 0, 0, 0])),
                (
                    COL_INGESTED_TIMESTAMP,
                    build_micros_column(&[MARCH_1, JULY_1, MARCH_1, MARCH_1, MARCH_1]),
                ),
                (
                    OPERATION_NAME,
                    build_string_column(&["op1", "op2", "op3", "op4", "op-b"]),
                ),
                (
                    PROVIDER_NAME,
                    build_string_column(&["openai", "openai", "openai", "openai", "openai"]),
                ),
                (
                    REQUEST_MODEL,
                    build_string_column(&["gpt-4o", "gpt-4o", "gpt-4o", "unknown-model", "gpt-4o"]),
                ),
                (INPUT_TOKENS, build_long_column(&[1000, 1000, 5000, 1000, 1000])),
                (OUTPUT_TOKENS, build_long_column(&[200, 200, 100, 200, 200])),
            ],
        )
        .await
        .expect("commit operations");

        let priced_query = "source=operations \
             | fields operation_name, span_id, timestamp, provider_name, request_model, input_tokens, output_tokens \
             | left join max=0 left=o right=p on o.provider_name = p.provider and o.request_model = p.model \
               and o.timestamp >= p.valid_from and (o.timestamp < p.valid_to or isnull(p.valid_to)) \
               and o.input_tokens >= p.min_input_tokens \
               [ source=prices_effective | where service_tier = 'standard' and region = 'global' \
                 | fields provider, model, valid_from, valid_to, min_input_tokens, \
                 input_usd_per_1m, output_usd_per_1m ] \
             | sort - valid_from, - min_input_tokens | dedup span_id \
             | eval cost_usd = if(isnull(input_usd_per_1m), 0.0D, \
                 input_tokens * input_usd_per_1m / 1000000.0D + output_tokens * output_usd_per_1m / 1000000.0D) \
             | fields operation_name, cost_usd | sort operation_name";
        let answer = run_ppl(&server, TENANT_A, priced_query).await;
        assert_eq!(
            answer["schema"],
            json!([
                build_schema_column("operation_name", "string"),
                build_schema_column("cost_usd", "double")
            ])
        );
        // op1 (March, 1000 in / 200 out) is priced by January's 2.5 / 10: 1000 * 2.5 / 1e6 + 200 * 10 / 1e6,
        // which IEEE double arithmetic evaluates to 0.0045000000000000005. op2 (July) by June's 3 / 15:
        // 0.006. op3 (5000 in) matches January's two tiers; the 2000-token tier sorts first and wins:
        // 5000 * 2 / 1e6 + 100 * 8 / 1e6 = 0.0108. op4's model has no price: 0.0. Tenant B's op-b never shows.
        assert_eq!(
            answer["datarows"],
            json!([
                ["op1", 0.004_500_000_000_000_000_5],
                ["op2", 0.006],
                ["op3", 0.0108],
                ["op4", 0.0]
            ])
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn the_join_right_side_never_reads_another_tenants_rows() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![
                        (COL_BODY, Cell::Text("a-log")),
                        (COL_SERVICE_NAME, Cell::Text("checkout")),
                    ],
                ),
                build_log_row(
                    TENANT_B,
                    NOON,
                    vec![
                        (COL_BODY, Cell::Text("b-log")),
                        (COL_SERVICE_NAME, Cell::Text("checkout")),
                    ],
                ),
            ],
        )
        .await
        .expect("commit logs");
        // One WAL segment file holds both tenants' spans; only tenant B has a `checkout` span.
        write_wal_segment(
            &server.wal_store,
            &catalog,
            SPANS_TABLE,
            0,
            &[
                build_span_row(TENANT_A, NOON, "cart", "a-span"),
                build_span_row(TENANT_B, NOON, "checkout", "b-span"),
            ],
        )
        .await
        .expect("write spans segment");
        let format_join_query = |kind: &str| {
            format!(
                "source=logs | fields body, service_name | {kind} join left=l right=s \
                 on l.service_name = s.service_name [ source=spans | fields name, service_name ] | fields body, name"
            )
        };

        assert_eq!(
            run_ppl(&server, TENANT_A, &format_join_query("left")).await["datarows"],
            json!([["a-log", null]])
        );
        assert_eq!(
            run_ppl(&server, TENANT_A, &format_join_query("inner")).await["datarows"],
            json!([])
        );
        assert_eq!(
            run_ppl(&server, TENANT_B, &format_join_query("inner")).await["datarows"],
            json!([["b-log", "b-span"]])
        );

        // A right side over a system index is refused like a system-index source, naming the index.
        create_system_index(&server, TENANT_A).await.expect("create the system index");
        let system_side = refuse_ppl(
            &server,
            TENANT_A,
            &format!("source=logs | inner join right=k on body = k.x [ source={SYSTEM_INDEX} ]"),
        )
        .await;
        let system_error = &system_side.parse_json_body()["error"];
        assert_semantic_error(system_error);
        assert!(
            system_error["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains(SYSTEM_INDEX)),
            "{}",
            system_side.text
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn an_ill_typed_join_criteria_is_a_semantic_400() {
        let (server, catalog) = TestServer::start().await;
        commit_equality_rows(&catalog).await.expect("commit the equality rows");
        // A MAP compared with a timestamp fits no comparison type.
        let reply = refuse_ppl(
            &server,
            TENANT_A,
            "source=logs | inner join left=l right=s on l.log_attributes = s.timestamp [ source=spans ]",
        )
        .await;
        assert_semantic_error(&reply.parse_json_body()["error"]);

        // Criteria of a string type are no predicate.
        let string_criteria_reply = refuse_ppl(
            &server,
            TENANT_A,
            "source=logs | inner join left=l right=s on concat(l.body, s.name) [ source=spans ]",
        )
        .await;
        assert_semantic_error(&string_criteria_reply.parse_json_body()["error"]);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn a_join_side_over_ten_thousand_rows_is_never_capped() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[build_log_row(
                TENANT_A,
                NOON,
                vec![(COL_BODY, Cell::Text("l1")), (COL_SERVICE_NAME, Cell::Text("checkout"))],
            )],
        )
        .await
        .expect("commit logs");
        let names: Vec<String> = (0..10_050).map(|index| format!("s{index}")).collect();
        let mut spans: Vec<Row<'_>> = names
            .iter()
            .zip(0_i64..)
            .map(|(name, offset)| build_span_row(TENANT_A, NOON + offset, "checkout", name))
            .collect();
        spans.extend((0..5).map(|offset| build_span_row(TENANT_B, NOON + offset, "checkout", "b-span")));
        commit_rows(&catalog, SPANS_TABLE, &spans).await.expect("commit spans");

        let query = "source=logs | fields body, service_name \
             | inner join left=l right=s on l.service_name = s.service_name \
             [ source=spans | fields name, service_name ] \
             | stats count() as n";
        assert_eq!(run_ppl(&server, TENANT_A, query).await["datarows"], json!([[10_050]]));
        server.shutdown().await;
    }
}

/// Subsearches: `where … in [ … ]` as a semi-join and `source=[ … ]`, through the tenant scope, uncapped.
mod subsearch {
    use icegate_common::schema::{COL_BODY, COL_SEVERITY_TEXT, COL_TRACE_ID};

    use super::*;

    const TRACE_ONE: [u8; 16] = [0x01; 16];
    const TRACE_TWO: [u8; 16] = [0x02; 16];
    const TRACE_THREE: [u8; 16] = [0x03; 16];

    /// A distinct trace id per index: the index in the first eight bytes, big-endian.
    fn build_indexed_trace_id(index: u64) -> [u8; 16] {
        let mut id = [0_u8; 16];
        id[..8].copy_from_slice(&index.to_be_bytes());
        id
    }

    fn build_traced_span_row<'a>(tenant: &'a str, trace_id: &'a [u8], name: &'a str) -> Row<'a> {
        let mut row = build_span_row(tenant, NOON, "checkout", name);
        row.cells.push((COL_TRACE_ID, Cell::Bytes(trace_id)));
        row
    }

    const IN_SUBSEARCH: &str = "source=logs | where trace_id in [ source=spans | where name = 'match' | fields trace_id ] | fields body | sort body";

    /// The rows come sorted before the `where`, and the subsearch sits beside another `and` conjunct: the
    /// semi-join keeps no row order, so the sort must still hold after it.
    const SORTED_IN_SUBSEARCH: &str = "source=logs | sort - body \
         | where body != 'zz' and trace_id in [ source=spans | where name = 'match' | fields trace_id ] \
         | fields body";

    #[tokio::test]
    async fn where_in_a_subsearch_plans_an_uncapped_semi_join_and_keeps_the_sort_order() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("l1")), (COL_TRACE_ID, Cell::Bytes(&TRACE_ONE))],
                ),
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("l2")), (COL_TRACE_ID, Cell::Bytes(&TRACE_TWO))],
                ),
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("l3")), (COL_TRACE_ID, Cell::Bytes(&TRACE_THREE))],
                ),
            ],
        )
        .await
        .expect("commit logs");
        // `TRACE_ONE` matches twice in the subsearch and still yields one `l1`.
        commit_rows(
            &catalog,
            SPANS_TABLE,
            &[
                build_traced_span_row(TENANT_A, &TRACE_ONE, "match"),
                build_traced_span_row(TENANT_A, &TRACE_ONE, "match"),
                build_traced_span_row(TENANT_A, &TRACE_TWO, "match"),
                build_traced_span_row(TENANT_A, &TRACE_THREE, "other"),
            ],
        )
        .await
        .expect("commit spans");

        assert_eq!(
            run_ppl(&server, TENANT_A, IN_SUBSEARCH).await["datarows"],
            json!([["l1"], ["l2"]])
        );
        assert_eq!(
            run_ppl(&server, TENANT_A, SORTED_IN_SUBSEARCH).await["datarows"],
            json!([["l2"], ["l1"]])
        );

        let (_logical_plan, physical_plan) = explain_ppl(&server, TENANT_A, IN_SUBSEARCH).await;
        let semi_join_count = find_plan_lines(&physical_plan, "HashJoinExec")
            .into_iter()
            .filter(|line| line.contains("Semi"))
            .count();
        assert_eq!(semi_join_count, 1, "one semi-join:\n{physical_plan}");
        for limit in ["GlobalLimitExec", "LocalLimitExec", "fetch="] {
            assert!(
                find_plan_lines(&physical_plan, limit).is_empty(),
                "no cap ({limit}):\n{physical_plan}"
            );
        }
        server.shutdown().await;
    }

    #[tokio::test]
    async fn an_in_subsearch_matching_more_than_ten_thousand_rows_is_never_capped() {
        let (server, catalog) = TestServer::start().await;
        let trace_ids: Vec<[u8; 16]> = (0..10_053).map(build_indexed_trace_id).collect();
        let logs: Vec<Row<'_>> = trace_ids
            .iter()
            .zip(0_i64..)
            .map(|(trace_id, offset)| {
                build_log_row(TENANT_A, NOON + offset, vec![(COL_TRACE_ID, Cell::Bytes(trace_id))])
            })
            .collect();
        commit_rows(&catalog, LOGS_TABLE, &logs).await.expect("commit logs");
        // Tenant A's subsearch matches the first 10 050 ids; the last three exist only as tenant B spans.
        let mut spans: Vec<Row<'_>> = trace_ids[..10_050]
            .iter()
            .map(|trace_id| build_traced_span_row(TENANT_A, trace_id, "match"))
            .collect();
        spans.extend(
            trace_ids[10_050..]
                .iter()
                .map(|trace_id| build_traced_span_row(TENANT_B, trace_id, "match")),
        );
        commit_rows(&catalog, SPANS_TABLE, &spans).await.expect("commit spans");

        let query = "source=logs | where trace_id in [ source=spans | where name = 'match' | fields trace_id ] \
             | stats count() as n";
        assert_eq!(run_ppl(&server, TENANT_A, query).await["datarows"], json!([[10_050]]));
        server.shutdown().await;
    }

    #[tokio::test]
    async fn a_subsearch_source_with_an_alias_feeds_the_pipeline() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![
                        (COL_BODY, Cell::Text("e1")),
                        (COL_SEVERITY_TEXT, Cell::Text("ERROR")),
                        (COL_SERVICE_NAME, Cell::Text("checkout")),
                    ],
                ),
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![
                        (COL_BODY, Cell::Text("i1")),
                        (COL_SEVERITY_TEXT, Cell::Text("INFO")),
                        (COL_SERVICE_NAME, Cell::Text("checkout")),
                    ],
                ),
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![
                        (COL_BODY, Cell::Text("e2")),
                        (COL_SEVERITY_TEXT, Cell::Text("ERROR")),
                        (COL_SERVICE_NAME, Cell::Text("cart")),
                    ],
                ),
            ],
        )
        .await
        .expect("commit logs");
        commit_rows(
            &catalog,
            SPANS_TABLE,
            &[build_span_row(TENANT_A, NOON, "checkout", "s-checkout")],
        )
        .await
        .expect("commit spans");
        let errors = "[ source=logs | where severity_text = 'ERROR' | fields body, service_name ]";

        let count_query = format!("source={errors} as t | stats count() as n");
        assert_eq!(run_ppl(&server, TENANT_A, &count_query).await["datarows"], json!([[2]]));

        let join_query = format!(
            "source={errors} as t | inner join right=s on t.service_name = s.service_name \
             [ source=spans | fields name, service_name ] | fields body, name"
        );
        assert_eq!(
            run_ppl(&server, TENANT_A, &join_query).await["datarows"],
            json!([["e1", "s-checkout"]])
        );

        let unaliased_query = format!("source={errors} | fields body | sort body");
        assert_eq!(
            run_ppl(&server, TENANT_A, &unaliased_query).await["datarows"],
            json!([["e1"], ["e2"]])
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn an_alias_on_an_index_source_is_a_semantic_400() {
        let (server, _catalog) = TestServer::start().await;
        let reply = refuse_ppl(&server, TENANT_A, "source=logs as l | fields body").await;
        assert_semantic_refusal(&reply, "left=<alias>");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn a_subsearch_column_of_another_type_is_a_semantic_400() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[build_log_row(TENANT_A, NOON, vec![(COL_BODY, Cell::Text("a"))])],
        )
        .await
        .expect("commit logs");
        commit_rows(
            &catalog,
            SPANS_TABLE,
            &[build_span_row(TENANT_A, NOON, "checkout", "s1")],
        )
        .await
        .expect("commit spans");
        // Positive control: the same subsearch against a string column answers, so the 400 below
        // comes from the column type alone.
        run_ppl(
            &server,
            TENANT_A,
            "source=logs | where body in [ source=spans | fields name ]",
        )
        .await;
        // A MAP compared with the subsearch's string column fits no comparison type.
        let reply = refuse_ppl(
            &server,
            TENANT_A,
            "source=logs | where log_attributes in [ source=spans | fields name ]",
        )
        .await;
        assert_semantic_error(&reply.parse_json_body()["error"]);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn the_subsearch_never_reads_another_tenants_rows() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("l1")), (COL_SERVICE_NAME, Cell::Text("checkout"))],
                ),
                build_log_row(
                    TENANT_A,
                    NOON,
                    vec![(COL_BODY, Cell::Text("l2")), (COL_SERVICE_NAME, Cell::Text("cart"))],
                ),
            ],
        )
        .await
        .expect("commit logs");
        // One WAL segment file: tenant A has a `checkout` span, only tenant B a `cart` span.
        write_wal_segment(
            &server.wal_store,
            &catalog,
            SPANS_TABLE,
            0,
            &[
                build_span_row(TENANT_A, NOON, "checkout", "a-span"),
                build_span_row(TENANT_B, NOON, "cart", "b-span"),
            ],
        )
        .await
        .expect("write spans segment");

        let filtered_query = "source=logs | where service_name in [ source=spans | fields service_name ] | fields body";
        assert_eq!(
            run_ppl(&server, TENANT_A, filtered_query).await["datarows"],
            json!([["l1"]])
        );
        let source_query = "source=[ source=spans | fields name ] | fields name";
        assert_eq!(
            run_ppl(&server, TENANT_A, source_query).await["datarows"],
            json!([["a-span"]])
        );
        assert_eq!(
            run_ppl(&server, TENANT_B, source_query).await["datarows"],
            json!([["b-span"]])
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn misplaced_wide_and_system_index_subsearches_are_semantic_400s() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[build_log_row(
                TENANT_A,
                NOON,
                vec![(COL_BODY, Cell::Text("l1")), (COL_SERVICE_NAME, Cell::Text("checkout"))],
            )],
        )
        .await
        .expect("commit logs");
        commit_rows(
            &catalog,
            SPANS_TABLE,
            &[build_span_row(TENANT_A, NOON, "checkout", "a-span")],
        )
        .await
        .expect("commit spans");

        let nested_reply = refuse_ppl(
            &server,
            TENANT_A,
            "source=logs | where body = 'x' or service_name in [ source=spans | fields service_name ]",
        )
        .await;
        assert_semantic_refusal(&nested_reply, "top-level");
        let two_column_reply = refuse_ppl(
            &server,
            TENANT_A,
            "source=logs | where service_name in [ source=spans | fields name, service_name ]",
        )
        .await;
        assert_semantic_refusal(&two_column_reply, "one column");

        // A subsearch over a system index is refused like a system-index source, naming the index.
        create_system_index(&server, TENANT_A).await.expect("create the system index");
        for query in [
            format!("source=logs | where body in [ source={SYSTEM_INDEX} | fields x ]"),
            format!("source=[ source={SYSTEM_INDEX} ]"),
        ] {
            let system_reply = refuse_ppl(&server, TENANT_A, &query).await;
            assert_semantic_refusal(&system_reply, SYSTEM_INDEX);
        }
        server.shutdown().await;
    }
}

/// `eventstats` and `streamstats`: aggregates per group added to every row, in the incoming order.
mod window {
    use icegate_common::schema::{COL_BODY, COL_SEVERITY_TEXT};
    use icegate_query::engine::QueryEngineConfig;

    use super::*;

    fn build_logged_row<'a>(
        tenant: &'a str,
        second: i64,
        body: &'a str,
        service: Option<&'a str>,
        severity: &'a str,
    ) -> Row<'a> {
        let mut cells = vec![(COL_BODY, Cell::Text(body)), (COL_SEVERITY_TEXT, Cell::Text(severity))];
        if let Some(service) = service {
            cells.push((COL_SERVICE_NAME, Cell::Text(service)));
        }
        build_log_row(tenant, NOON + second * SECOND, cells)
    }

    /// A server over the tenant-A logs `a`–`e`, logged 1–5 seconds after noon: `a` and `c` in `checkout`,
    /// `b` in `cart`, `d` and `e` without a service, and only `c` at `ERROR`; plus a tenant-B `checkout` row
    /// `z` in the same WAL file as `d` and `e`, which no tenant-A query may count.
    ///
    /// # Errors
    ///
    /// The write error when a row cannot be committed or the WAL segment cannot be written.
    async fn start_grouped_logs_server() -> Result<TestServer, RowWriteError> {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[
                build_logged_row(TENANT_A, 1, "a", Some("checkout"), "INFO"),
                build_logged_row(TENANT_A, 2, "b", Some("cart"), "INFO"),
                build_logged_row(TENANT_A, 3, "c", Some("checkout"), "ERROR"),
            ],
        )
        .await?;
        write_wal_segment(
            &server.wal_store,
            &catalog,
            LOGS_TABLE,
            1,
            &[
                build_logged_row(TENANT_A, 4, "d", None, "INFO"),
                build_logged_row(TENANT_A, 5, "e", None, "INFO"),
                build_logged_row(TENANT_B, 6, "z", Some("checkout"), "INFO"),
            ],
        )
        .await?;
        Ok(server)
    }

    #[tokio::test]
    async fn eventstats_adds_group_aggregates_to_every_row_with_bucket_nullable() {
        let server = start_grouped_logs_server().await.expect("start the grouped logs server");

        let format_grouped_query = |options: &str| {
            format!(
                "source=logs | sort body | eventstats {options} count() as n, max(timestamp) as newest by service_name \
                 | fields body, service_name, n, newest"
            )
        };
        let nullable_answer = run_ppl(&server, TENANT_A, &format_grouped_query("")).await;
        assert_eq!(
            nullable_answer["schema"],
            json!([
                build_schema_column("body", "string"),
                build_schema_column("service_name", "string"),
                build_schema_column("n", "bigint"),
                build_schema_column("newest", "timestamp")
            ])
        );
        assert_eq!(
            nullable_answer["datarows"],
            json!([
                ["a", "checkout", 2, "2026-10-08 12:00:03"],
                ["b", "cart", 1, "2026-10-08 12:00:02"],
                ["c", "checkout", 2, "2026-10-08 12:00:03"],
                ["d", null, 2, "2026-10-08 12:00:05"],
                ["e", null, 2, "2026-10-08 12:00:05"]
            ])
        );
        assert_eq!(
            run_ppl(&server, TENANT_A, &format_grouped_query("bucket_nullable=true")).await["datarows"],
            nullable_answer["datarows"]
        );

        let non_nullable_answer = run_ppl(&server, TENANT_A, &format_grouped_query("bucket_nullable=false")).await;
        assert_eq!(
            non_nullable_answer["datarows"],
            json!([
                ["a", "checkout", 2, "2026-10-08 12:00:03"],
                ["b", "cart", 1, "2026-10-08 12:00:02"],
                ["c", "checkout", 2, "2026-10-08 12:00:03"],
                ["d", null, null, null],
                ["e", null, null, null]
            ])
        );

        let whole_query = "source=logs | sort - body | eventstats count() as total | fields body, total";
        assert_eq!(
            run_ppl(&server, TENANT_A, whole_query).await["datarows"],
            json!([["e", 5], ["d", 5], ["c", 5], ["b", 5], ["a", 5]])
        );

        // An aggregate named like an input column replaces it in place: `service_name` stays second, now a
        // count. INFO holds a, b, d and e.
        let replacing_query = "source=logs | fields body, service_name, severity_text | sort body \
             | eventstats count() as service_name by severity_text | head 1";
        let replacing_answer = run_ppl(&server, TENANT_A, replacing_query).await;
        assert_eq!(
            replacing_answer["schema"],
            json!([
                build_schema_column("body", "string"),
                build_schema_column("service_name", "bigint"),
                build_schema_column("severity_text", "string")
            ])
        );
        assert_eq!(replacing_answer["datarows"], json!([["a", 4, "INFO"]]));
        server.shutdown().await;
    }

    #[tokio::test]
    async fn the_ordinal_eventstats_records_survives_fields_and_fillnull_for_a_later_streamstats() {
        let server = start_grouped_logs_server().await.expect("start the grouped logs server");

        // `fields` keeps the hidden ordinal `eventstats` recorded for the later `streamstats`; `fillnull`'s
        // all-fields form skips it and fills the visible string columns only.
        let filled_query = "source=logs | sort body | eventstats count() as n by service_name \
             | fields body, service_name | fillnull with 'none' | streamstats count() as rn \
             | fields body, service_name, rn";
        assert_eq!(
            run_ppl(&server, TENANT_A, filled_query).await["datarows"],
            json!([
                ["a", "checkout", 1],
                ["b", "cart", 2],
                ["c", "checkout", 3],
                ["d", "none", 4],
                ["e", "none", 5]
            ])
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn window_commands_refuse_duplicate_names_unsupported_functions_and_replaced_promoted_columns() {
        let server = start_grouped_logs_server().await.expect("start the grouped logs server");

        for command in ["eventstats", "streamstats"] {
            let twice_reply = refuse_ppl(
                &server,
                TENANT_A,
                &format!("source=logs | {command} count() as n, max(timestamp) as n"),
            )
            .await;
            assert_semantic_refusal(&twice_reply, &format!("{command} names the column `n` twice"));

            let distinct_reply =
                refuse_ppl(&server, TENANT_A, &format!("source=logs | {command} dc(service_name)")).await;
            assert_semantic_refusal(&distinct_reply, "`dc`");

            // An aggregate that replaces the column a promoted attribute reads leaves that attribute unreadable.
            let replaced_reply = refuse_ppl(
                &server,
                TENANT_A,
                &format!(
                    "source=logs | {command} count() as service_name | where resource_attributes.service.name = 'api'"
                ),
            )
            .await;
            assert_semantic_refusal(
                &replaced_reply,
                "field [resource_attributes.service.name] reads the column [service_name], which an earlier command \
                 removed, renamed or replaced",
            );
        }
        server.shutdown().await;
    }

    #[tokio::test]
    async fn eventstats_computes_every_supported_aggregate_and_refuses_span_keys() {
        let server = start_grouped_logs_server().await.expect("start the grouped logs server");

        // `elapsed` is a row's second after noon (a=1 … e=5). `count(service_name)` counts non-NULL values, so
        // the NULL group of d and e counts 0; `a` and `b` name one aggregate twice and share its window column.
        let aggregates_query = "source=logs | sort body \
             | eval elapsed = timestampdiff(SECOND, timestamp('2026-10-08 12:00:00'), timestamp) \
             | eventstats count(service_name) as services, sum(elapsed) as elapsed_sum, \
               avg(elapsed) as elapsed_avg, min(elapsed) as elapsed_min, count() as a, count() as b by service_name \
             | fields body, services, elapsed_sum, elapsed_avg, elapsed_min, a, b";
        let aggregates_answer = run_ppl(&server, TENANT_A, aggregates_query).await;
        assert_eq!(
            aggregates_answer["schema"],
            json!([
                build_schema_column("body", "string"),
                build_schema_column("services", "bigint"),
                build_schema_column("elapsed_sum", "bigint"),
                build_schema_column("elapsed_avg", "double"),
                build_schema_column("elapsed_min", "bigint"),
                build_schema_column("a", "bigint"),
                build_schema_column("b", "bigint")
            ])
        );
        assert_eq!(
            aggregates_answer["datarows"],
            json!([
                ["a", 2, 4, 2.0, 1, 2, 2],
                ["b", 1, 2, 2.0, 2, 1, 1],
                ["c", 2, 4, 2.0, 1, 2, 2],
                ["d", 0, 9, 4.5, 4, 2, 2],
                ["e", 0, 9, 4.5, 4, 2, 2]
            ])
        );

        let span_reply = refuse_ppl(
            &server,
            TENANT_A,
            "source=logs | eventstats count() by span(timestamp, 1h)",
        )
        .await;
        assert_semantic_refusal(
            &span_reply,
            "eventstats groups by fields; `by span(…)` is not supported",
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn streamstats_count_by_numbers_rows_in_their_sorted_order() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[
                build_logged_row(TENANT_A, 1, "a", Some("checkout"), "INFO"),
                build_logged_row(TENANT_A, 3, "c", Some("checkout"), "INFO"),
                build_logged_row(TENANT_A, 5, "e", Some("cart"), "INFO"),
            ],
        )
        .await
        .expect("commit logs");
        write_wal_segment(
            &server.wal_store,
            &catalog,
            LOGS_TABLE,
            1,
            &[
                build_logged_row(TENANT_A, 2, "b", Some("cart"), "INFO"),
                build_logged_row(TENANT_A, 4, "d", Some("checkout"), "INFO"),
                build_logged_row(TENANT_B, 3, "z", Some("checkout"), "INFO"),
            ],
        )
        .await
        .expect("write logs segment");

        let ascending_query =
            "source=logs | sort timestamp | streamstats count() as rn by service_name | fields body, service_name, rn";
        assert_eq!(
            run_ppl(&server, TENANT_A, ascending_query).await["datarows"],
            json!([
                ["a", "checkout", 1],
                ["b", "cart", 1],
                ["c", "checkout", 2],
                ["d", "checkout", 3],
                ["e", "cart", 2]
            ])
        );
        let descending_query = "source=logs | sort - timestamp | streamstats count() as rn by service_name | fields body, service_name, rn";
        assert_eq!(
            run_ppl(&server, TENANT_A, descending_query).await["datarows"],
            json!([
                ["e", "cart", 1],
                ["d", "checkout", 1],
                ["c", "checkout", 2],
                ["b", "cart", 2],
                ["a", "checkout", 3]
            ])
        );
        let first_per_group_query =
            "source=logs | sort timestamp | streamstats count() as rn by service_name | where rn = 1 | fields body";
        assert_eq!(
            run_ppl(&server, TENANT_A, first_per_group_query).await["datarows"],
            json!([["a"], ["b"]])
        );
        let running_query = "source=logs | sort timestamp | streamstats count() as seen | fields body, seen";
        assert_eq!(
            run_ppl(&server, TENANT_A, running_query).await["datarows"],
            json!([["a", 1], ["b", 2], ["c", 3], ["d", 4], ["e", 5]])
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn a_where_after_eventstats_or_streamstats_never_moves_below_them() {
        let (server, catalog) = TestServer::start().await;
        commit_rows(
            &catalog,
            LOGS_TABLE,
            &[
                build_logged_row(TENANT_A, 1, "a", Some("checkout"), "INFO"),
                build_logged_row(TENANT_A, 2, "b", Some("cart"), "ERROR"),
                build_logged_row(TENANT_A, 3, "c", Some("checkout"), "ERROR"),
                // Read by tenant A, `z` would be a third `checkout` row and a third answer row.
                build_logged_row(TENANT_B, 4, "z", Some("checkout"), "ERROR"),
            ],
        )
        .await
        .expect("commit logs");
        // A filter moved below either command would make `c` the first and only `checkout` row: 1, not 2.
        let event_query = "source=logs | sort body | eventstats count() as n by service_name \
             | where severity_text = 'ERROR' | fields body, n";
        assert_eq!(
            run_ppl(&server, TENANT_A, event_query).await["datarows"],
            json!([["b", 1], ["c", 2]])
        );
        let stream_query = "source=logs | sort body | streamstats count() as rn by service_name \
             | where severity_text = 'ERROR' | fields body, rn";
        assert_eq!(
            run_ppl(&server, TENANT_A, stream_query).await["datarows"],
            json!([["b", 1], ["c", 2]])
        );

        for query in [event_query, stream_query] {
            let (logical_plan, _physical_plan) = explain_ppl(&server, TENANT_A, query).await;
            let lines: Vec<&str> = logical_plan.lines().collect();
            let filter_line = lines
                .iter()
                .position(|line| line.contains("Filter:") && line.contains("severity_text"))
                .unwrap_or_else(|| panic!("no severity filter:\n{logical_plan}"));
            let window_line = lines
                .iter()
                .position(|line| line.contains("WindowAggr"))
                .unwrap_or_else(|| panic!("no window:\n{logical_plan}"));
            assert!(
                filter_line < window_line,
                "the filter stays above the window:\n{logical_plan}"
            );
            assert!(
                find_plan_lines(&logical_plan, "TableScan")
                    .iter()
                    .all(|line| !line.contains("ERROR")),
                "the filter never reaches the scan:\n{logical_plan}"
            );
        }
        server.shutdown().await;
    }

    #[tokio::test]
    async fn eventstats_over_more_than_ten_thousand_rows_is_never_capped() {
        let (server, catalog) = TestServer::start().await;
        let mut rows: Vec<Row<'_>> = (0..10_050)
            .map(|offset| {
                build_log_row(
                    TENANT_A,
                    NOON + offset,
                    vec![(COL_SERVICE_NAME, Cell::Text("checkout"))],
                )
            })
            .collect();
        rows.extend((0..3).map(|offset| {
            build_log_row(
                TENANT_B,
                NOON + offset,
                vec![(COL_SERVICE_NAME, Cell::Text("checkout"))],
            )
        }));
        commit_rows(&catalog, LOGS_TABLE, &rows).await.expect("commit logs");

        let query =
            "source=logs | eventstats count() as n by service_name | stats count() as rows, max(n) as group_size";
        assert_eq!(
            run_ppl(&server, TENANT_A, query).await["datarows"],
            json!([[10_050, 10_050]])
        );
        server.shutdown().await;
    }

    /// The even rows are one Iceberg commit and the odd rows one WAL segment, each more rows than the engine's
    /// batch size, so the scans deliver them in batches of either side; only the order written into the
    /// numbering window puts them in time order. Row `k` is at `NOON + k` µs with body `k`, so its rank is
    /// `k + 1`. A tenant-B row one microsecond before row 0 in the Iceberg commit would take rank 1 and make the
    /// total 18001 if tenant A read it.
    #[tokio::test]
    async fn streamstats_follows_the_sorted_order_across_iceberg_and_wal_beyond_one_batch() {
        assert!(
            ROWS_PER_SIDE > QueryEngineConfig::default().batch_size,
            "each side must span more than one record batch"
        );
        let (server, catalog) = TestServer::start().await;
        let bodies = build_numbered_bodies(2 * ROWS_PER_SIDE);
        let mut iceberg_rows = build_numbered_rows(&bodies, 0);
        iceberg_rows.push(build_log_row(
            TENANT_B,
            NOON - 1,
            vec![(COL_BODY, Cell::Text("tenant-b"))],
        ));
        commit_rows(&catalog, LOGS_TABLE, &iceberg_rows)
            .await
            .expect("commit the even rows and the tenant-B row");
        write_wal_segment(
            &server.wal_store,
            &catalog,
            LOGS_TABLE,
            1,
            &build_numbered_rows(&bodies, 1),
        )
        .await
        .expect("write the odd rows");

        let ascending_query = "source=logs | sort timestamp | streamstats count() as rn \
             | where rn = 1 or rn = 2 or rn = 9001 or rn = 18000 | fields body, rn";
        assert_eq!(
            run_ppl(&server, TENANT_A, ascending_query).await["datarows"],
            json!([["00000", 1], ["00001", 2], ["09000", 9001], ["17999", 18000]])
        );

        // `fields` keeps the hidden ordinal `eventstats` left behind, so `streamstats` still numbers the rows in
        // descending order.
        let chained_query = "source=logs | sort - timestamp | eventstats count() as total | fields body, total \
             | streamstats count() as rn | where rn = 1 or rn = 2 or rn = 9001 or rn = 18000 | fields body, rn, total";
        assert_eq!(
            run_ppl(&server, TENANT_A, chained_query).await["datarows"],
            json!([
                ["17999", 1, 18000],
                ["17998", 2, 18000],
                ["08999", 9001, 18000],
                ["00000", 18000, 18000]
            ])
        );
        server.shutdown().await;
    }
}
