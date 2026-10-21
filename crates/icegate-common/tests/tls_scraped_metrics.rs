//! The names under which the certificate reload metrics are scraped.
//!
//! Operators alert on the scraped series, not on the instrument names the code registers:
//! the Prometheus exporter every service uses derives the scraped name from the instrument
//! name, its unit and its kind. This pins the result, so a rename of an instrument, or a
//! change in the exporter's naming, fails here rather than silencing an alert.
//!
//! An integration test of its own because [`MetricsRuntime`] installs the process-wide meter
//! provider, and every case that does so needs a process to itself.
//!
//! ```text
//! cargo test -p icegate-common --test tls_scraped_metrics
//! ```
#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use icegate_common::{CancellationToken, MetricsRuntime, ServerCertificate, TlsConfig};
use prometheus::{Encoder, TextEncoder};
use rcgen::{CertificateParams, KeyPair};

const CERTIFICATE_FILE: &str = "tls.crt";
const PRIVATE_KEY_FILE: &str = "tls.key";

const RELOAD_INTERVAL_SECS: u64 = 1;
const WAIT_TIMEOUT: Duration = Duration::from_secs(20);

/// The scraped series of the reloads counter and the expiry gauge, as an operator writes
/// them in a query. Stated as literals so the expectation shares nothing with the code that
/// registers the instruments.
const SCRAPED_RELOADS_COUNTER: &str = "icegate_tls_certificate_reloads_total";
const SCRAPED_EXPIRY_GAUGE: &str = "icegate_tls_certificate_not_after_seconds";

/// Write a fresh self-signed pair for `localhost` into `dir`, replacing any pair there.
fn write_pair(dir: &Path) {
    let key_pair = KeyPair::generate().expect("generate key pair");
    let certificate = CertificateParams::new(vec!["localhost".to_string()])
        .expect("certificate params")
        .self_signed(&key_pair)
        .expect("self-sign certificate");
    fs::write(dir.join(CERTIFICATE_FILE), certificate.pem()).expect("write certificate");
    fs::write(dir.join(PRIVATE_KEY_FILE), key_pair.serialize_pem()).expect("write private key");
}

/// What a scrape of the registry returns: the Prometheus text exposition.
fn scrape(metrics: &MetricsRuntime) -> String {
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metrics.registry().gather(), &mut buffer)
        .expect("encode the registry");
    String::from_utf8(buffer).expect("the exposition is UTF-8")
}

/// The `# TYPE <name> <type>` declarations of a scrape, by series name.
fn read_declared_types(exposition: &str) -> BTreeMap<String, String> {
    exposition
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|declaration| declaration.split_once(' '))
        .map(|(name, kind)| (name.to_string(), kind.to_string()))
        .collect()
}

/// Whether a scrape holds a sample line for the series `name`, with or without labels.
fn has_sample(exposition: &str, name: &str) -> bool {
    exposition.lines().any(|line| {
        line.strip_prefix(name)
            .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('{'))
    })
}

#[tokio::test]
async fn the_reload_counter_and_expiry_gauge_are_scraped_under_their_documented_names() {
    let metrics = MetricsRuntime::new("tls-scraped-metrics").expect("the Prometheus exporter every service uses");
    let dir = tempfile::tempdir().expect("temp dir");
    write_pair(dir.path());
    let tls = TlsConfig {
        certificate_path: dir.path().join(CERTIFICATE_FILE),
        private_key_path: dir.path().join(PRIVATE_KEY_FILE),
        reload_interval_secs: RELOAD_INTERVAL_SECS,
    };
    let certificate = Arc::new(ServerCertificate::load(&tls).expect("load the pair"));
    let cancel_token = CancellationToken::new();
    let watcher = tokio::spawn(Arc::clone(&certificate).watch_files(cancel_token.clone()));

    // The counter has no series until a reload is counted, and a renewed pair is what the
    // watcher counts.
    write_pair(dir.path());
    let mut exposition = String::new();
    let both_series_are_scraped = async {
        loop {
            exposition = scrape(&metrics);
            if has_sample(&exposition, SCRAPED_RELOADS_COUNTER) && has_sample(&exposition, SCRAPED_EXPIRY_GAUGE) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let outcome = tokio::time::timeout(WAIT_TIMEOUT, both_series_are_scraped).await;
    assert!(
        outcome.is_ok(),
        "both series must appear in a scrape within a few reload intervals; the last scrape:\n{exposition}"
    );

    let declared_types = read_declared_types(&exposition);
    assert_eq!(
        declared_types.get(SCRAPED_RELOADS_COUNTER).map(String::as_str),
        Some("counter"),
        "declared: {declared_types:?}"
    );
    assert_eq!(
        declared_types.get(SCRAPED_EXPIRY_GAUGE).map(String::as_str),
        Some("gauge"),
        "declared: {declared_types:?}"
    );

    cancel_token.cancel();
    tokio::time::timeout(WAIT_TIMEOUT, watcher)
        .await
        .expect("the watcher must stop once cancelled")
        .expect("the watcher must not panic");
}
