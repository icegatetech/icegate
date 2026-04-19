//! Planner unit tests over a small in-memory spans table.
//!
//! These tests mount a [`MemTable`] under the fully-qualified spans-table
//! name (`iceberg.icegate.spans`) so the planner's `session_ctx.table(...)`
//! resolves without spinning up a real Iceberg catalog. The fixture holds
//! three spans across two tenants; each test re-creates the session so the
//! tests are independent.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array, Int64Array, MapBuilder, StringArray, StringBuilder, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider},
    datasource::MemTable,
    prelude::SessionContext,
};
use icegate_common::SPANS_TABLE_FQN;

use super::DataFusionPlanner;
use crate::{
    error::QueryError,
    traceql::{
        antlr::AntlrParser,
        parser::Parser,
        planner::{Planner, QueryContext},
    },
};

/// Build the spans-table fixture schema.
///
/// Mirrors the relevant subset of [`icegate_common::schema::spans_schema`] —
/// everything the planner reads. The MAP element field names match
/// [`MapBuilder`]'s defaults (`entries`/`keys`/`values`) so the schema
/// matches the array type produced by [`make_fixture_batch`] exactly.
///
/// Two separate MAP columns (`resource_attributes`, `span_attributes`)
/// mirror the post-2026-04-19 spans schema split.
fn fixture_schema() -> Arc<Schema> {
    fn map_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Utf8, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        )
    }

    Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Int32, true),
        Field::new("status_code", DataType::Int32, true),
        Field::new("duration_micros", DataType::Int64, true),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        map_field("resource_attributes"),
        map_field("span_attributes"),
    ]))
}

/// Build the three-span fixture: two `t1` spans (one ok, one error), one
/// `t2` span. Timestamps cluster around "now" so any reasonable
/// [`QueryContext`] window covers them.
///
/// `resource_attributes` holds `service.name` and `k8s.namespace.name` for
/// every row. `span_attributes` holds `http.method` for every row, plus
/// `only.span = "yes"` on the middle row (the error span) so tests can
/// assert span-only routing.
fn make_fixture_batch(schema: Arc<Schema>) -> RecordBatch {
    let tenants = StringArray::from(vec!["t1", "t1", "t2"]);
    // service_name column is populated at ingest from resource.service.name;
    // both t1 spans use "frontend", t2 uses "backend".
    let service_names = StringArray::from(vec![Some("frontend"), Some("frontend"), Some("backend")]);
    let trace_ids = StringArray::from(vec!["t1-trace-1", "t1-trace-2", "t2-trace-1"]);
    let span_ids = StringArray::from(vec!["s1", "s2", "s3"]);
    let names = StringArray::from(vec!["GET /a", "GET /b", "POST /c"]);
    // OTel SpanKind: 2 = SERVER, 3 = CLIENT.
    let kinds = Int32Array::from(vec![Some(2), Some(2), Some(3)]);
    // OTel StatusCode: 1 = OK, 2 = ERROR.
    let statuses = Int32Array::from(vec![Some(1), Some(2), Some(1)]);
    // Durations in microseconds: 100ms, 5s, 250ms.
    let durations = Int64Array::from(vec![Some(100_000), Some(5_000_000), Some(250_000)]);
    let now = Utc::now().timestamp_micros();
    let timestamps = TimestampMicrosecondArray::from(vec![now - 1000, now - 500, now - 200]);

    // resource_attributes: every row has service.name + k8s.namespace.name.
    let mut res_b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for svc in ["frontend", "frontend", "backend"] {
        res_b.keys().append_value("service.name");
        res_b.values().append_value(svc);
        res_b.keys().append_value("k8s.namespace.name");
        res_b.values().append_value("icegate");
        res_b.append(true).expect("resource_attrs row");
    }
    let resource_attrs: ArrayRef = Arc::new(res_b.finish());

    // span_attributes: every row has http.method; the middle (error) row
    // additionally has `only.span = yes` so we can test span-only keys.
    let mut span_b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for (method, extra) in [("GET", None), ("GET", Some(("only.span", "yes"))), ("POST", None)] {
        span_b.keys().append_value("http.method");
        span_b.values().append_value(method);
        if let Some((k, v)) = extra {
            span_b.keys().append_value(k);
            span_b.values().append_value(v);
        }
        span_b.append(true).expect("span_attrs row");
    }
    let span_attrs: ArrayRef = Arc::new(span_b.finish());

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(tenants),
            Arc::new(service_names),
            Arc::new(trace_ids),
            Arc::new(span_ids),
            Arc::new(names),
            Arc::new(kinds),
            Arc::new(statuses),
            Arc::new(durations),
            Arc::new(timestamps),
            resource_attrs,
            span_attrs,
        ],
    )
    .expect("record batch")
}

/// Register `table` under a three-part FQN by creating the catalog/schema
/// hierarchy on the fly. Equivalent in spirit to the `LogQL` planner's Iceberg
/// catalog registration — just `MemTable`-backed.
fn register_under_fqn(ctx: &SessionContext, fqn: &str, table: Arc<MemTable>) {
    let parts: Vec<&str> = fqn.split('.').collect();
    assert_eq!(parts.len(), 3, "FQN must be catalog.schema.table");
    let (catalog_name, schema_name, table_name) = (parts[0], parts[1], parts[2]);

    let catalog = ctx.catalog(catalog_name).unwrap_or_else(|| {
        let provider: Arc<dyn CatalogProvider> = Arc::new(MemoryCatalogProvider::new());
        ctx.register_catalog(catalog_name, provider);
        ctx.catalog(catalog_name).expect("catalog just registered")
    });

    let schema = catalog.schema(schema_name).unwrap_or_else(|| {
        let provider: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
        // `register_schema` returns the *previous* schema (or `None`); re-fetch
        // to obtain the just-registered provider.
        let _ = catalog.register_schema(schema_name, provider).expect("register schema");
        catalog.schema(schema_name).expect("schema just registered")
    });

    schema.register_table(table_name.to_string(), table).expect("register table");
}

/// Build a fresh [`SessionContext`] with the spans fixture mounted at
/// [`SPANS_TABLE_FQN`].
fn fixture_session() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = fixture_schema();
    let batch = make_fixture_batch(schema.clone());
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
    register_under_fqn(&ctx, SPANS_TABLE_FQN, Arc::new(table));
    ctx
}

/// Wide time window that covers any "now-ish" timestamp the fixture emits.
fn make_query_ctx(tenant: &str) -> QueryContext {
    QueryContext {
        tenant_id: tenant.to_string(),
        start: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        end: Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap(),
        limit: None,
        min_duration: None,
        max_duration: None,
        step: None,
        max_grid_points: QueryContext::DEFAULT_MAX_GRID_POINTS,
    }
}

/// Plan `query` against `ctx` + `qctx`, collect all batches, and return the
/// total row count. Used by the attribute-routing tests to assert the
/// number of matching rows without boilerplate in every test body.
async fn run_and_count(ctx: SessionContext, qctx: QueryContext, query: &str) -> usize {
    let expr = AntlrParser::new().parse(query).expect("parse");
    let planner = DataFusionPlanner::new(ctx, qctx);
    planner
        .plan(expr)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect")
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

// =========================================================================
// Search tests
// =========================================================================

#[tokio::test]
async fn empty_selector_returns_only_tenant_t1_rows() {
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, "{}").await;
    assert_eq!(total, 2);
}

#[tokio::test]
async fn status_eq_error_filters_to_one_row() {
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, "{ status = error }").await;
    assert_eq!(total, 1);
}

#[tokio::test]
async fn duration_gt_returns_only_slow_spans() {
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, "{ duration > 1s }").await;
    assert_eq!(total, 1);
}

#[tokio::test]
async fn pipeline_count_filter_groups_by_trace() {
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    // Default group key is `trace_id`; tenant `t1` has two distinct trace IDs,
    // each with count=1, so both rows pass `count() > 0`.
    let total = run_and_count(ctx, qctx, "{} | count() > 0").await;
    assert_eq!(total, 2);
}

// =========================================================================
// Attribute filters (regression: map_extract returns List<Utf8>)
// =========================================================================
//
// `map_extract(attributes, key)` returns `List<Utf8>`; the planner must
// unwrap via `array_element(..., 1)` so `=`/`!=` coerce against the RHS
// literal. Without the unwrap the session fails with
// "Cannot infer common argument type for comparison operation List(Utf8) = Utf8".

#[tokio::test]
async fn resource_service_name_quoted_eq_returns_matching_rows() {
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, r#"{ resource.service.name = "frontend" }"#).await;
    // Both t1 spans share service.name=frontend.
    assert_eq!(total, 2);
}

#[tokio::test]
async fn resource_service_name_bare_eq_behaves_like_quoted() {
    // Regression: unquoted RHS (as Grafana sends) must not trip type coercion.
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, "{resource.service.name=frontend}").await;
    assert_eq!(total, 2);
}

#[tokio::test]
async fn missing_attribute_value_yields_no_rows() {
    // Key not present in map -> array_element returns NULL -> predicate false.
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, r#"{ resource.service.name = "does-not-exist" }"#).await;
    assert_eq!(total, 0);
}

#[tokio::test]
async fn span_scope_filter_uses_span_attributes_column() {
    // `span.only.span = yes` should match only the middle row (the "error" span
    // that has the extra span-only attribute in the fixture).
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, r#"{ span.only.span = "yes" }"#).await;
    assert_eq!(total, 1);
}

#[tokio::test]
async fn resource_scope_filter_uses_resource_attributes_column() {
    // `resource.k8s.namespace.name = "icegate"` should match all t1 rows
    // (both fixture rows have this resource-level key).
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, r#"{ resource.k8s.namespace.name = "icegate" }"#).await;
    assert_eq!(total, 2);
}

#[tokio::test]
async fn any_scope_dot_shorthand_matches_either_map() {
    // `.only.span = "yes"` — the key lives only in span_attributes, and the
    // any-scope OR should still match.
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, r#"{ .only.span = "yes" }"#).await;
    assert_eq!(total, 1);
}

#[tokio::test]
async fn any_scope_also_matches_resource_keys() {
    // `.k8s.namespace.name = "icegate"` — key in resource_attributes only.
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, r#"{ .k8s.namespace.name = "icegate" }"#).await;
    assert_eq!(total, 2);
}

#[tokio::test]
async fn span_scope_service_name_does_not_short_circuit_to_column() {
    // Regression: `span.service.name = "frontend"` must look in span_attributes,
    // NOT the top-level service_name column. Since the fixture's
    // span_attributes map never contains service.name, this returns 0 rows.
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let total = run_and_count(ctx, qctx, r#"{ span.service.name = "frontend" }"#).await;
    assert_eq!(total, 0);
}

// =========================================================================
// NotImplemented rejections
// =========================================================================

#[tokio::test]
async fn descendant_operator_returns_not_implemented() {
    let ctx = fixture_session();
    let qctx = make_query_ctx("t1");
    let expr = AntlrParser::new()
        .parse("{ kind = server } >> { kind = client }")
        .expect("parse");
    let planner = DataFusionPlanner::new(ctx, qctx);
    let err = planner.plan(expr).await.expect_err("descendant must reject");
    assert!(matches!(err, QueryError::NotImplemented(_)));
}
