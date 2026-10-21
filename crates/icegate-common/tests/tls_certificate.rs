//! Loading, validating and hot-reloading the server certificate from PEM files.
//!
//! An integration test because every case writes PEM files to a temporary
//! directory, which `docs/tests.md` keeps out of unit tests.
//!
//! ```text
//! cargo test -p icegate-common --test tls_certificate
//! ```
#![allow(clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use icegate_common::{CancellationToken, CertificateReload, Error, ServerCertificate, TlsConfig};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use rcgen::{CertificateParams, DnType, KeyPair};
use time::OffsetDateTime;

// Unix seconds of 2040-01-01, 2041-01-01 and 2042-01-01 (UTC). Stated as numbers so
// the expectations do not share a conversion with the code under test.
const NOT_AFTER_2040: u64 = 2_208_988_800;
const NOT_AFTER_2041: u64 = 2_240_611_200;
const NOT_AFTER_2042: u64 = 2_272_147_200;

const CERTIFICATE_FILE: &str = "tls.crt";
const PRIVATE_KEY_FILE: &str = "tls.key";

const RELOAD_INTERVAL_SECS: u64 = 1;
const WAIT_TIMEOUT: Duration = Duration::from_secs(20);

/// Write a fresh self-signed pair into `dir`, replacing the previous pair there.
fn write_pair(dir: &Path, common_name: &str, not_after_unix: u64) -> (PathBuf, PathBuf) {
    let key_pair = KeyPair::generate().expect("generate key pair");
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).expect("certificate params");
    params.distinguished_name.push(DnType::CommonName, common_name);
    params.not_after = OffsetDateTime::from_unix_timestamp(i64::try_from(not_after_unix).expect("fits i64"))
        .expect("valid unix timestamp");
    let certificate = params.self_signed(&key_pair).expect("self-sign certificate");

    let certificate_path = dir.join(CERTIFICATE_FILE);
    let private_key_path = dir.join(PRIVATE_KEY_FILE);
    fs::write(&certificate_path, certificate.pem()).expect("write certificate");
    fs::write(&private_key_path, key_pair.serialize_pem()).expect("write private key");
    (certificate_path, private_key_path)
}

fn build_config(certificate_path: &Path, private_key_path: &Path) -> TlsConfig {
    TlsConfig {
        certificate_path: certificate_path.to_path_buf(),
        private_key_path: private_key_path.to_path_buf(),
        reload_interval_secs: RELOAD_INTERVAL_SECS,
    }
}

fn to_unix_seconds(instant: SystemTime) -> u64 {
    instant.duration_since(UNIX_EPOCH).expect("instant after the epoch").as_secs()
}

/// A loaded certificate over a pair expiring at `not_after_unix`, with the pair's files.
fn load_pair(dir: &Path, not_after_unix: u64) -> (ServerCertificate, PathBuf, PathBuf) {
    let (certificate_path, private_key_path) = write_pair(dir, "first", not_after_unix);
    let certificate = ServerCertificate::load(&build_config(&certificate_path, &private_key_path)).expect("load pair");
    (certificate, certificate_path, private_key_path)
}

#[test]
fn load_serves_the_configured_pair() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (certificate, ..) = load_pair(dir.path(), NOT_AFTER_2040);

    assert_eq!(to_unix_seconds(certificate.not_after()), NOT_AFTER_2040);
}

#[test]
fn load_rejects_a_missing_certificate_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (certificate_path, private_key_path) = write_pair(dir.path(), "first", NOT_AFTER_2040);
    fs::remove_file(&certificate_path).expect("remove certificate");

    let result = ServerCertificate::load(&build_config(&certificate_path, &private_key_path));

    assert!(matches!(result, Err(Error::Tls(_))), "got {result:?}");
}

#[test]
fn load_rejects_a_key_that_does_not_match_the_certificate() {
    let dir = tempfile::tempdir().expect("temp dir");
    let other_dir = tempfile::tempdir().expect("temp dir");
    let (certificate_path, _) = write_pair(dir.path(), "first", NOT_AFTER_2040);
    let (_, foreign_key_path) = write_pair(other_dir.path(), "second", NOT_AFTER_2040);

    let result = ServerCertificate::load(&build_config(&certificate_path, &foreign_key_path));

    assert!(matches!(result, Err(Error::Tls(_))), "got {result:?}");
}

#[test]
fn reload_reports_unchanged_when_the_files_did_not_change() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (certificate, ..) = load_pair(dir.path(), NOT_AFTER_2040);

    let outcome = certificate.reload().expect("reload");

    assert_eq!(outcome, CertificateReload::Unchanged);
    assert_eq!(to_unix_seconds(certificate.not_after()), NOT_AFTER_2040);
}

#[test]
fn reload_replaces_the_certificate_when_the_pair_changes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (certificate, ..) = load_pair(dir.path(), NOT_AFTER_2040);
    write_pair(dir.path(), "renewed", NOT_AFTER_2041);

    let outcome = certificate.reload().expect("reload");

    assert_eq!(outcome, CertificateReload::Replaced);
    assert_eq!(to_unix_seconds(certificate.not_after()), NOT_AFTER_2041);
}

#[test]
fn reload_keeps_the_previous_certificate_when_the_key_does_not_match() {
    let dir = tempfile::tempdir().expect("temp dir");
    let other_dir = tempfile::tempdir().expect("temp dir");
    let (certificate, certificate_path, _) = load_pair(dir.path(), NOT_AFTER_2040);
    let (foreign_certificate_path, _) = write_pair(other_dir.path(), "second", NOT_AFTER_2041);
    // Only the certificate is replaced, so the key left in place no longer matches it.
    fs::copy(&foreign_certificate_path, &certificate_path).expect("overwrite certificate");

    let result = certificate.reload();

    assert!(matches!(result, Err(Error::Tls(_))), "got {result:?}");
    assert_eq!(to_unix_seconds(certificate.not_after()), NOT_AFTER_2040);
}

#[test]
fn reload_recovers_once_the_matching_key_arrives_after_a_rejected_certificate() {
    let dir = tempfile::tempdir().expect("temp dir");
    let other_dir = tempfile::tempdir().expect("temp dir");
    let (certificate, certificate_path, private_key_path) = load_pair(dir.path(), NOT_AFTER_2040);
    let (renewed_certificate_path, renewed_private_key_path) = write_pair(other_dir.path(), "renewed", NOT_AFTER_2041);
    // The state a reader sees when it lands between the two file updates of a renewal.
    fs::copy(&renewed_certificate_path, &certificate_path).expect("overwrite certificate");
    assert!(matches!(certificate.reload(), Err(Error::Tls(_))));

    fs::copy(&renewed_private_key_path, &private_key_path).expect("overwrite private key");
    let outcome = certificate.reload().expect("reload");

    assert_eq!(outcome, CertificateReload::Replaced);
    assert_eq!(to_unix_seconds(certificate.not_after()), NOT_AFTER_2041);
}

#[test]
fn reload_keeps_the_previous_certificate_when_a_file_is_missing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (certificate, _, private_key_path) = load_pair(dir.path(), NOT_AFTER_2040);
    fs::remove_file(&private_key_path).expect("remove private key");

    let result = certificate.reload();

    assert!(matches!(result, Err(Error::Tls(_))), "got {result:?}");
    assert_eq!(to_unix_seconds(certificate.not_after()), NOT_AFTER_2040);
}

#[test]
fn reload_keeps_the_previous_certificate_when_the_certificate_file_is_truncated() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (certificate, certificate_path, _) = load_pair(dir.path(), NOT_AFTER_2040);
    // A reader that lands while a renewal is still writing the file sees a prefix of it.
    let certificate_bytes = fs::read(&certificate_path).expect("read certificate");
    fs::write(&certificate_path, &certificate_bytes[..certificate_bytes.len() / 2]).expect("truncate certificate");

    let result = certificate.reload();

    assert!(matches!(result, Err(Error::Tls(_))), "got {result:?}");
    assert_eq!(to_unix_seconds(certificate.not_after()), NOT_AFTER_2040);
}

#[test]
fn validate_rejects_an_empty_certificate_path() {
    let config = TlsConfig {
        certificate_path: PathBuf::new(),
        private_key_path: PathBuf::from("/tls/tls.key"),
        reload_interval_secs: RELOAD_INTERVAL_SECS,
    };

    let result = config.validate();

    assert!(matches!(result, Err(Error::Config(_))), "got {result:?}");
}

#[test]
fn validate_rejects_a_zero_reload_interval() {
    let config = TlsConfig {
        certificate_path: PathBuf::from("/tls/tls.crt"),
        private_key_path: PathBuf::from("/tls/tls.key"),
        reload_interval_secs: 0,
    };

    let result = config.validate();

    assert!(matches!(result, Err(Error::Config(_))), "got {result:?}");
}

/// Meter provider recording into memory; measurements reach the exporter on `force_flush`.
fn build_meter_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone()).build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    (provider, exporter)
}

/// Cumulative total of the reload counter for the data point labelled `outcome`, from the
/// last export that carried it; zero when it was never incremented with that label.
fn read_reload_total(exporter: &InMemoryMetricExporter, outcome: &str) -> u64 {
    let mut latest = 0;
    for batch in exporter.get_finished_metrics().expect("finished metrics") {
        for scope in batch.scope_metrics() {
            for metric in scope
                .metrics()
                .filter(|metric| metric.name() == "icegate_tls_certificate_reloads")
            {
                let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                    panic!("the reload counter must be a u64 sum");
                };
                for point in sum.data_points() {
                    let labels: Vec<_> = point
                        .attributes()
                        .map(|attribute| (attribute.key.as_str(), attribute.value.as_str().into_owned()))
                        .collect();
                    if labels == [("outcome", outcome.to_string())] {
                        latest = point.value();
                    }
                }
            }
        }
    }
    latest
}

/// The last value the gauge `icegate_tls_certificate_not_after` was exported with.
fn read_not_after_gauge(exporter: &InMemoryMetricExporter) -> Option<u64> {
    let mut latest = None;
    for batch in exporter.get_finished_metrics().expect("finished metrics") {
        for scope in batch.scope_metrics() {
            for metric in scope
                .metrics()
                .filter(|metric| metric.name() == "icegate_tls_certificate_not_after")
            {
                let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
                    panic!("the not-after metric must be a u64 gauge");
                };
                latest = gauge
                    .data_points()
                    .map(opentelemetry_sdk::metrics::data::GaugeDataPoint::value)
                    .last();
            }
        }
    }
    latest
}

/// Poll `condition` (after flushing the meter provider) until it holds, within [`WAIT_TIMEOUT`].
async fn wait_for_metrics(
    description: &str,
    provider: &SdkMeterProvider,
    exporter: &InMemoryMetricExporter,
    condition: impl Fn(&InMemoryMetricExporter) -> bool + Send + Sync,
) {
    let wait = async {
        loop {
            provider.force_flush().expect("flush metrics");
            if condition(exporter) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(WAIT_TIMEOUT, wait)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {description}"));
}

#[tokio::test]
async fn watch_files_counts_each_reload_outcome_and_stops_when_cancelled() {
    let (provider, exporter) = build_meter_provider();
    opentelemetry::global::set_meter_provider(provider.clone());
    let dir = tempfile::tempdir().expect("temp dir");
    let other_dir = tempfile::tempdir().expect("temp dir");
    let (certificate, certificate_path, _) = load_pair(dir.path(), NOT_AFTER_2040);
    let certificate = Arc::new(certificate);
    let cancel_token = CancellationToken::new();
    let watcher = tokio::spawn(Arc::clone(&certificate).watch_files(cancel_token.clone()));

    wait_for_metrics("the loaded not-after", &provider, &exporter, |exporter| {
        read_not_after_gauge(exporter) == Some(NOT_AFTER_2040)
    })
    .await;

    write_pair(dir.path(), "renewed", NOT_AFTER_2041);
    wait_for_metrics("the renewed pair to be counted", &provider, &exporter, |exporter| {
        read_reload_total(exporter, "replaced") >= 1 && read_not_after_gauge(exporter) == Some(NOT_AFTER_2041)
    })
    .await;
    assert_eq!(to_unix_seconds(certificate.not_after()), NOT_AFTER_2041);

    // A renewal write can be observed half-done (new certificate, old key), which is
    // itself a rejection, so only growth past the count seen here proves this one.
    let rejected_total_before = read_reload_total(&exporter, "rejected");
    let (foreign_certificate_path, _) = write_pair(other_dir.path(), "foreign", NOT_AFTER_2042);
    fs::copy(&foreign_certificate_path, &certificate_path).expect("overwrite certificate");
    wait_for_metrics(
        "the mismatched certificate to be rejected",
        &provider,
        &exporter,
        |exporter| read_reload_total(exporter, "rejected") > rejected_total_before,
    )
    .await;
    assert_eq!(to_unix_seconds(certificate.not_after()), NOT_AFTER_2041);
    assert_eq!(read_not_after_gauge(&exporter), Some(NOT_AFTER_2041));
    assert_eq!(
        read_reload_total(&exporter, "replaced"),
        1,
        "a rejected pair must not count as a replacement"
    );

    cancel_token.cancel();
    tokio::time::timeout(WAIT_TIMEOUT, watcher)
        .await
        .expect("the watcher must stop once cancelled")
        .expect("the watcher must not panic");
}
