//! Tests for the Tempo trace-lookup endpoints: `/api/traces/{traceID}`
//! returns a bare OTLP payload, `/api/v2/traces/{traceID}` returns the same
//! payload inside the `TraceByIDResponse` envelope. The two body shapes are
//! not interchangeable — a decoder following one contract cannot read the
//! other — so each route is pinned to its own shape here.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use icegate_common::{ICEGATE_NAMESPACE, SPANS_TABLE};
use opentelemetry_proto::tonic::trace::v1::TracesData;
use prost::Message;
use serde_json::Value;

use super::harness::{TestServer, write_test_spans};

/// `trace_id` of the root span written by [`write_test_spans`].
const FIXTURE_TRACE_ID: &str = "0102030405060708090a0b0c0d0e0f10";

/// Unwrap a `TraceByIDResponse` body into the trace it embeds.
///
/// Walks the wire form by hand — envelope key, length, payload — instead of
/// decoding the body as a `TracesData`, so a v2 route that regressed to the
/// bare v1 payload fails here rather than silently decoding. Only valid for a
/// complete trace: the length check assumes nothing follows the trace field,
/// which is what `status`/`message` would add for a truncated one.
fn decode_envelope_trace(body: &[u8]) -> TracesData {
    let mut buf = body;
    let (tag, wire_type) = prost::encoding::decode_key(&mut buf).expect("envelope key");
    assert_eq!(tag, 1, "TraceByIDResponse.trace is field 1");
    assert_eq!(wire_type, prost::encoding::WireType::LengthDelimited);
    let len = prost::encoding::decode_varint(&mut buf).expect("trace length");
    assert_eq!(len, u64::try_from(buf.len()).expect("length fits u64"));
    TracesData::decode(buf).expect("embedded trace is a TracesData")
}

#[tokio::test]
async fn v1_returns_bare_otlp_traces_data() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    let resp = server
        .client
        .get(format!("{}/api/traces/{FIXTURE_TRACE_ID}", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .header("Accept", "application/protobuf")
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await?;

    let traces = TracesData::decode(body).expect("v1 body is a TracesData");
    let spans = &traces.resource_spans[0].scope_spans[0].spans;
    assert_eq!(spans.len(), 1, "only the root span carries this trace id");
    assert_eq!(spans[0].name, "GET /api/health");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn v2_wraps_trace_in_trace_by_id_response() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    let resp = server
        .client
        .get(format!("{}/api/v2/traces/{FIXTURE_TRACE_ID}", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .header("Accept", "application/protobuf")
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await?;

    let traces = decode_envelope_trace(&body);
    let spans = &traces.resource_spans[0].scope_spans[0].spans;
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name, "GET /api/health");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn v2_json_nests_trace_under_envelope() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    let resp = server
        .client
        .get(format!("{}/api/v2/traces/{FIXTURE_TRACE_ID}", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .header("Accept", "application/json")
        .send()
        .await?;
    let status = resp.status();
    let body: Value = resp.json().await?;
    assert_eq!(status, 200, "Response body: {body}");

    assert_eq!(body["status"], "COMPLETE");
    assert_eq!(
        body["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"],
        "GET /api/health"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn v2_returns_404_for_unknown_trace() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    // An unknown trace is a 404, not a 200 carrying an empty envelope: the
    // status code is the only signal a caller has before decoding the body.
    let resp = server
        .client
        .get(format!(
            "{}/api/v2/traces/ffffffffffffffffffffffffffffffff",
            server.base_url
        ))
        .header("X-Scope-OrgID", "tempo-tenant")
        .header("Accept", "application/protobuf")
        .send()
        .await?;
    assert_eq!(resp.status(), 404);

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn v2_isolates_trace_lookup_by_tenant() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;
    let table_ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?;
    // The same trace id lives under both tenants, so a lookup that stopped
    // filtering by tenant would return two root spans instead of one below,
    // and 200 instead of 404 for a tenant that wrote nothing.
    write_test_spans(&catalog.load_table(&table_ident).await?, &catalog, "tempo-tenant").await?;
    // Reload between writes: the first commit advanced the table's snapshot,
    // and the append is planned against the metadata it was handed.
    write_test_spans(&catalog.load_table(&table_ident).await?, &catalog, "other-tenant").await?;

    let owner = server
        .client
        .get(format!("{}/api/v2/traces/{FIXTURE_TRACE_ID}", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .header("Accept", "application/protobuf")
        .send()
        .await?;
    assert_eq!(owner.status(), 200);
    let traces = decode_envelope_trace(&owner.bytes().await?);
    let span_count: usize = traces
        .resource_spans
        .iter()
        .flat_map(|rs| rs.scope_spans.iter())
        .map(|ss| ss.spans.len())
        .sum();
    assert_eq!(span_count, 1, "only the requesting tenant's root span");

    let stranger = server
        .client
        .get(format!("{}/api/v2/traces/{FIXTURE_TRACE_ID}", server.base_url))
        .header("X-Scope-OrgID", "third-tenant")
        .header("Accept", "application/protobuf")
        .send()
        .await?;
    assert_eq!(stranger.status(), 404, "a trace of another tenant must not be readable");

    server.shutdown().await;
    Ok(())
}
