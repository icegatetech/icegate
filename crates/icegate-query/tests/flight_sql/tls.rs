//! The Flight SQL listener over TLS, at the real transport boundary.
#![allow(clippy::expect_used)]

use datafusion::arrow::array::Int64Array;

use super::harness::{TestServer, execute_sql};
use crate::tls_certificate::TestCertificate;

/// The harness channel trusts only the test certificate and connects over `https`, so a query
/// that round-trips shows the handshake completed with that certificate and `h2` was
/// negotiated, which gRPC requires.
#[tokio::test]
async fn flight_sql_answers_over_tls() {
    let certificate = TestCertificate::generate();
    let (server, _catalog) = TestServer::start_with_certificate(Some(&certificate))
        .await
        .expect("start the Flight SQL test server over TLS");
    let mut client = server.client(Some("default"));

    let batches = execute_sql(&mut client, "SELECT 1 AS one").await.expect("a query over TLS");
    server.shutdown().await;

    assert_eq!(batches.len(), 1, "expected exactly one batch");
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("SELECT 1 produces an Int64 column")
        .value(0);
    assert_eq!(value, 1);
}
