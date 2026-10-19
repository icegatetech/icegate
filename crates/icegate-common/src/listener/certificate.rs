//! The server certificate a TLS listener presents, re-read from disk without a restart.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge};
use rustls::crypto::aws_lc_rs;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::time::{Instant, MissedTickBehavior};

use crate::CancellationToken;
use crate::error::{CommonError, Result};
use crate::listener::TlsConfig;

/// Meter name for every instrument published for the served certificate.
const METER_NAME: &str = "icegate-tls";

const OUTCOME_LABEL: &str = "outcome";
const OUTCOME_REPLACED: &str = "replaced";
const OUTCOME_REJECTED: &str = "rejected";

/// Seconds since the Unix epoch; zero for an instant before it.
fn to_unix_seconds(instant: SystemTime) -> u64 {
    instant.duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_secs())
}

/// What [`ServerCertificate::reload`] did with the files it read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertificateReload {
    /// Both files hold the bytes already being served, so nothing was parsed.
    Unchanged,
    /// A new, valid pair is served from the next handshake on.
    Replaced,
}

/// The raw bytes of the two PEM files, exactly as last read.
///
/// Kept next to the parsed pair so a re-read can tell "nothing changed" by comparing
/// bytes, without parsing and without a clock. Deliberately not `Debug`: one of the
/// two is a private key.
#[derive(PartialEq, Eq)]
struct PemPair {
    certificate: Vec<u8>,
    private_key: Vec<u8>,
}

/// A pair the server is willing to present: parsed, matched, and ready for handshakes.
struct ServedCertificate {
    certified_key: Arc<CertifiedKey>,
    not_after: SystemTime,
    pem: PemPair,
}

// Manual so that no formatting of a served certificate can reach the key material.
impl fmt::Debug for ServedCertificate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServedCertificate")
            .field("not_after", &self.not_after)
            .finish_non_exhaustive()
    }
}

/// Where the pair lives on disk, and the one place that turns its bytes into a
/// [`ServedCertificate`] so every failure names the file it came from.
#[derive(Debug)]
struct PairFiles {
    certificate_path: PathBuf,
    private_key_path: PathBuf,
}

impl PairFiles {
    fn read_pem_pair(&self) -> Result<PemPair> {
        Ok(PemPair {
            certificate: read_file("certificate", &self.certificate_path)?,
            private_key: read_file("private key", &self.private_key_path)?,
        })
    }

    /// Accept `pem` only if its key signs for its leaf certificate.
    fn parse_pem_pair(&self, pem: PemPair) -> Result<ServedCertificate> {
        let chain = CertificateDer::pem_slice_iter(&pem.certificate)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                CommonError::Tls(format!(
                    "cannot parse certificate {}: {error}",
                    self.certificate_path.display()
                ))
            })?;
        let leaf = chain.first().ok_or_else(|| {
            CommonError::Tls(format!(
                "certificate {} contains no certificate",
                self.certificate_path.display()
            ))
        })?;
        let not_after = self.read_not_after(leaf)?;

        let private_key = PrivateKeyDer::from_pem_slice(&pem.private_key).map_err(|error| {
            CommonError::Tls(format!(
                "cannot parse private key {}: {error}",
                self.private_key_path.display()
            ))
        })?;
        let signing_key = aws_lc_rs::sign::any_supported_type(&private_key).map_err(|error| {
            CommonError::Tls(format!(
                "unsupported private key {}: {error}",
                self.private_key_path.display()
            ))
        })?;

        let certified_key = CertifiedKey::new(chain, signing_key);
        // Any error rejects the pair, including "cannot tell": a pair that cannot be
        // confirmed to match is not served.
        certified_key.keys_match().map_err(|error| {
            CommonError::Tls(format!(
                "certificate {} and private key {} are not a usable pair: {error}",
                self.certificate_path.display(),
                self.private_key_path.display()
            ))
        })?;

        Ok(ServedCertificate {
            certified_key: Arc::new(certified_key),
            not_after,
            pem,
        })
    }

    fn read_not_after(&self, leaf: &CertificateDer<'_>) -> Result<SystemTime> {
        let (_, certificate) = x509_parser::parse_x509_certificate(leaf.as_ref()).map_err(|error| {
            CommonError::Tls(format!(
                "certificate {} is not a valid X.509 certificate: {error}",
                self.certificate_path.display()
            ))
        })?;
        let seconds = u64::try_from(certificate.validity().not_after.timestamp()).map_err(|_| {
            CommonError::Tls(format!(
                "certificate {} expires before the Unix epoch",
                self.certificate_path.display()
            ))
        })?;
        Ok(UNIX_EPOCH + Duration::from_secs(seconds))
    }
}

fn read_file(role: &str, path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|error| CommonError::Tls(format!("cannot read {role} {}: {error}", path.display())))
}

/// The certificate and private key a TLS listener presents, kept current with the files
/// they were read from.
///
/// Handshakes read the served pair through a lock-free swap, so a reload never blocks
/// or tears one: each handshake presents either the whole old pair or the whole new one.
/// A pair is replaced only by another pair that parses and matches; whatever else the
/// files hold, the last good pair stays in service.
#[derive(Debug)]
pub struct ServerCertificate {
    files: PairFiles,
    reload_interval: Duration,
    // `ArcSwap` is an atomically replaceable `Arc`: readers `load()` a snapshot without
    // taking a lock. Shared with the resolver of every config from `build_server_config`.
    served_certificate: Arc<ArcSwap<ServedCertificate>>,
}

impl ServerCertificate {
    /// Read and validate the configured pair.
    ///
    /// # Errors
    /// * [`CommonError::Config`] when `config` fails [`TlsConfig::validate`].
    /// * [`CommonError::Tls`] when a file is missing or unreadable, holds unparsable PEM,
    ///   holds no certificate or no private key, or the key does not belong to the leaf
    ///   certificate. The message names the file.
    pub fn load(config: &TlsConfig) -> Result<Self> {
        config.validate()?;
        let files = PairFiles {
            certificate_path: config.certificate_path.clone(),
            private_key_path: config.private_key_path.clone(),
        };
        let served_certificate = files.parse_pem_pair(files.read_pem_pair()?)?;
        Ok(Self {
            files,
            reload_interval: Duration::from_secs(config.reload_interval_secs),
            served_certificate: Arc::new(ArcSwap::from_pointee(served_certificate)),
        })
    }

    /// Re-read both files and serve what they hold if it is a new, valid pair.
    ///
    /// Blocks on file I/O; an async caller moves it to a blocking thread, as
    /// [`Self::watch_files`] does.
    ///
    /// # Errors
    /// [`CommonError::Tls`] under the same conditions as [`Self::load`]. The pair being
    /// served is untouched, so a failed reload is safe to retry.
    pub fn reload(&self) -> Result<CertificateReload> {
        let pem = self.files.read_pem_pair()?;
        if pem == self.served_certificate.load().pem {
            return Ok(CertificateReload::Unchanged);
        }
        let served_certificate = self.files.parse_pem_pair(pem)?;
        self.served_certificate.store(Arc::new(served_certificate));
        Ok(CertificateReload::Replaced)
    }

    /// When the leaf certificate being served expires.
    #[must_use]
    pub fn not_after(&self) -> SystemTime {
        self.served_certificate.load().not_after
    }

    /// Re-read the files every `reload_interval_secs` until `cancel_token` is cancelled.
    ///
    /// Every reload that replaces the pair or fails is counted in
    /// `icegate_tls_certificate_reloads` under its `outcome`, and a failure is also
    /// logged; an unchanged pair is neither. `icegate_tls_certificate_not_after`
    /// carries the expiry of the pair in service from the moment this starts. These are
    /// the instrument names; the series an operator scrapes are named in [`TlsConfig`].
    ///
    /// A renewed pair is served within one interval after both files hold it. A read
    /// that lands between the two file updates sees a certificate that does not match
    /// its key and is rejected like any other bad pair; the next read picks the pair up.
    ///
    /// Returns at once, after logging an error, when the interval is too long for the
    /// clock to hold the first deadline. [`Self::load`] accepts no such interval, so this
    /// guards the arithmetic rather than a reachable configuration.
    pub async fn watch_files(self: Arc<Self>, cancel_token: CancellationToken) {
        let metrics = ReloadMetrics::new();
        metrics.record_not_after(self.not_after());

        // The files were read by `load` moments ago, so the first tick waits a full
        // interval. `interval_at` panics on a zero period, which `load` validated away,
        // and `Instant` addition panics on overflow, hence the checked form.
        let Some(first_tick) = Instant::now().checked_add(self.reload_interval) else {
            tracing::error!(
                reload_interval_secs = self.reload_interval.as_secs(),
                "TLS certificate reload interval overflows the clock; the served certificate will not be reloaded"
            );
            return;
        };
        let mut ticker = tokio::time::interval_at(first_tick, self.reload_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                () = cancel_token.cancelled() => return,
                _ = ticker.tick() => {}
            }

            // The blocking thread keeps file I/O off the reactor; its closure must own
            // everything it touches, hence its own `Arc` handle.
            let certificate = Arc::clone(&self);
            let result = tokio::task::spawn_blocking(move || certificate.reload())
                .await
                .unwrap_or_else(|error| Err(CommonError::Tls(format!("certificate reload task failed: {error}"))));

            match result {
                Ok(CertificateReload::Unchanged) => {}
                Ok(CertificateReload::Replaced) => {
                    let not_after = self.not_after();
                    metrics.record_reload(OUTCOME_REPLACED);
                    metrics.record_not_after(not_after);
                    tracing::info!(
                        not_after_unix_seconds = to_unix_seconds(not_after),
                        "replaced the served TLS certificate"
                    );
                }
                Err(error) => {
                    metrics.record_reload(OUTCOME_REJECTED);
                    tracing::error!(%error, "TLS certificate reload failed; still serving the previous certificate");
                }
            }
        }
    }

    /// The rustls configuration of one listener, resolving its certificate per handshake
    /// from whatever this holds at that moment.
    ///
    /// Built once per listener and shared by all its connections: a later [`Self::reload`]
    /// is picked up without rebuilding it. Offers no client authentication, and only
    /// the protocols in `alpn`, most preferred first.
    ///
    /// # Errors
    /// [`CommonError::Tls`] when the crypto provider cannot support the default protocol
    /// versions.
    pub(crate) fn build_server_config(&self, alpn: &[&[u8]]) -> Result<Arc<rustls::ServerConfig>> {
        // The provider is passed explicitly rather than read from a process-wide default,
        // so a TLS setup never depends on which crate installed one first.
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|error| CommonError::Tls(format!("unsupported TLS protocol versions: {error}")))?
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(ServedCertificateResolver {
                served_certificate: Arc::clone(&self.served_certificate),
            }));
        config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
        Ok(Arc::new(config))
    }
}

/// Hands each handshake the pair [`ServerCertificate`] is serving at that moment.
#[derive(Debug)]
struct ServedCertificateResolver {
    served_certificate: Arc<ArcSwap<ServedCertificate>>,
}

impl ResolvesServerCert for ServedCertificateResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.served_certificate.load().certified_key))
    }
}

/// The instruments [`ServerCertificate::watch_files`] reports through.
struct ReloadMetrics {
    reloads: Counter<u64>,
    not_after: Gauge<u64>,
}

impl ReloadMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter(METER_NAME);
        Self {
            reloads: meter
                .u64_counter("icegate_tls_certificate_reloads")
                .with_description("certificate reloads that replaced the served pair or were rejected, by outcome")
                .build(),
            not_after: meter
                .u64_gauge("icegate_tls_certificate_not_after")
                .with_description("expiry of the served leaf certificate, in seconds since the Unix epoch")
                .with_unit("s")
                .build(),
        }
    }

    fn record_reload(&self, outcome: &'static str) {
        self.reloads.add(1, &[KeyValue::new(OUTCOME_LABEL, outcome)]);
    }

    fn record_not_after(&self, not_after: SystemTime) {
        self.not_after.record(to_unix_seconds(not_after), &[]);
    }
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateParams, KeyPair, date_time_ymd};
    use rustls::RootCertStore;
    use rustls_pki_types::ServerName;
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    use super::*;

    /// A self-signed pair for `localhost`, generated in memory.
    ///
    /// `leaf_der` is the certificate as the generator emitted it, so a handshake can be
    /// checked against bytes that never went through the PEM decoding under test.
    struct GeneratedPair {
        pem_pair: PemPair,
        leaf_der: Vec<u8>,
    }

    fn generate_pair() -> GeneratedPair {
        generate_pair_with(|_| {})
    }

    /// Like [`generate_pair`], after `configure` has adjusted the certificate parameters.
    fn generate_pair_with(configure: impl FnOnce(&mut CertificateParams)) -> GeneratedPair {
        let key_pair = KeyPair::generate().expect("generate key pair");
        let mut params = CertificateParams::new(vec!["localhost".to_string()]).expect("certificate params");
        configure(&mut params);
        let certificate = params.self_signed(&key_pair).expect("self-sign certificate");
        GeneratedPair {
            pem_pair: PemPair {
                certificate: certificate.pem().into_bytes(),
                private_key: key_pair.serialize_pem().into_bytes(),
            },
            leaf_der: certificate.der().as_ref().to_vec(),
        }
    }

    fn build_pair_files() -> PairFiles {
        PairFiles {
            certificate_path: PathBuf::from("in-memory.crt"),
            private_key_path: PathBuf::from("in-memory.key"),
        }
    }

    fn build_certificate(pem_pair: PemPair) -> ServerCertificate {
        let files = build_pair_files();
        let served_certificate = files.parse_pem_pair(pem_pair).expect("parse generated pair");
        ServerCertificate {
            files,
            reload_interval: Duration::from_secs(1),
            served_certificate: Arc::new(ArcSwap::from_pointee(served_certificate)),
        }
    }

    /// Valid PEM framing around a body that is neither an X.509 certificate nor a key.
    fn wrap_garbage_in_pem(label: &str) -> Vec<u8> {
        // Base64 of arbitrary text, so the section decodes and only its content is wrong.
        format!("-----BEGIN {label}-----\nZ2FyYmFnZSBieXRlcw==\n-----END {label}-----\n").into_bytes()
    }

    /// What the client observed in one handshake.
    struct HandshakeOutcome {
        negotiated_protocol: Option<Vec<u8>>,
        presented_leaf: Vec<u8>,
    }

    /// Run one handshake against `acceptor` over an in-memory pipe, with a client that
    /// trusts exactly `trusted_leaves` and offers `h2`.
    async fn perform_handshake(acceptor: &TlsAcceptor, trusted_leaves: &[&[u8]]) -> HandshakeOutcome {
        let mut roots = RootCertStore::empty();
        for leaf in trusted_leaves {
            roots.add(CertificateDer::from(leaf.to_vec())).expect("trust the certificate");
        }
        let mut client_config = rustls::ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_config.alpn_protocols = vec![b"h2".to_vec()];

        let (client_io, server_io) = tokio::io::duplex(16 * 1024);
        let connector = TlsConnector::from(Arc::new(client_config));
        let server_name = ServerName::try_from("localhost").expect("server name");
        let (client, server) = tokio::join!(connector.connect(server_name, client_io), acceptor.accept(server_io));
        server.expect("server handshake");
        let client = client.expect("client handshake");

        let (_, connection) = client.get_ref();
        let presented_leaf = connection
            .peer_certificates()
            .and_then(|chain| chain.first())
            .expect("the server presents a certificate");
        HandshakeOutcome {
            negotiated_protocol: connection.alpn_protocol().map(<[u8]>::to_vec),
            presented_leaf: presented_leaf.as_ref().to_vec(),
        }
    }

    #[tokio::test]
    async fn server_config_presents_the_loaded_certificate_and_negotiates_the_offered_protocol() {
        let GeneratedPair { pem_pair, leaf_der } = generate_pair();
        let certificate = build_certificate(pem_pair);
        let acceptor = TlsAcceptor::from(certificate.build_server_config(&[b"h2"]).expect("server config"));

        let outcome = perform_handshake(&acceptor, &[&leaf_der]).await;

        assert_eq!(outcome.negotiated_protocol, Some(b"h2".to_vec()));
        assert_eq!(outcome.presented_leaf, leaf_der);
    }

    #[tokio::test]
    async fn a_server_config_built_before_a_swap_presents_the_swapped_certificate() {
        let first = generate_pair();
        let second = generate_pair();
        let trusted_leaves: [&[u8]; 2] = [&first.leaf_der, &second.leaf_der];
        let certificate = build_certificate(first.pem_pair);
        let acceptor = TlsAcceptor::from(certificate.build_server_config(&[b"h2"]).expect("server config"));
        let before_swap = perform_handshake(&acceptor, &trusted_leaves).await;

        let swapped_certificate = certificate.files.parse_pem_pair(second.pem_pair).expect("parse second pair");
        certificate.served_certificate.store(Arc::new(swapped_certificate));
        let after_swap = perform_handshake(&acceptor, &trusted_leaves).await;

        assert_eq!(before_swap.presented_leaf, trusted_leaves[0]);
        assert_eq!(after_swap.presented_leaf, trusted_leaves[1]);
    }

    /// `load` accepts no interval this long, so the value is planted past it: the watcher's
    /// deadline arithmetic must hold on its own, whatever reaches it.
    #[tokio::test]
    async fn watch_files_returns_instead_of_panicking_when_the_interval_overflows_the_clock() {
        let mut certificate = build_certificate(generate_pair().pem_pair);
        certificate.reload_interval = Duration::MAX;

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            Arc::new(certificate).watch_files(CancellationToken::new()),
        )
        .await;

        assert!(
            outcome.is_ok(),
            "the watcher must end rather than wait on an unreachable deadline"
        );
    }

    #[test]
    fn parse_pem_pair_rejects_every_malformed_pair_with_a_tls_error() {
        let valid = generate_pair().pem_pair;
        let expired_before_epoch = generate_pair_with(|params| {
            params.not_before = date_time_ymd(1950, 1, 1);
            params.not_after = date_time_ymd(1960, 1, 1);
        })
        .pem_pair
        .certificate;
        let unterminated_certificate = String::from_utf8(valid.certificate.clone())
            .expect("PEM is text")
            .replace("-----END CERTIFICATE-----", "")
            .into_bytes();
        // Every case breaks exactly one file of an otherwise valid pair, so the file left
        // alone is known good and the failure comes from the file that was broken.
        let broken_certificate = |certificate: Vec<u8>| PemPair {
            certificate,
            private_key: valid.private_key.clone(),
        };
        let broken_private_key = |private_key: Vec<u8>| PemPair {
            certificate: valid.certificate.clone(),
            private_key,
        };
        let cases = [
            ("empty certificate", broken_certificate(Vec::new())),
            (
                "certificate file holding plain text",
                broken_certificate(b"this is not a PEM file".to_vec()),
            ),
            (
                "certificate PEM missing its END line",
                broken_certificate(unterminated_certificate),
            ),
            (
                "certificate body that is not X.509",
                broken_certificate(wrap_garbage_in_pem("CERTIFICATE")),
            ),
            (
                "certificate that expired before the Unix epoch",
                broken_certificate(expired_before_epoch),
            ),
            (
                "private key file holding a certificate instead of a key",
                broken_private_key(valid.certificate.clone()),
            ),
            (
                "private key body that is not a key",
                broken_private_key(wrap_garbage_in_pem("PRIVATE KEY")),
            ),
        ];
        let files = build_pair_files();

        for (name, pem_pair) in cases {
            let result = files.parse_pem_pair(pem_pair);

            assert!(matches!(result, Err(CommonError::Tls(_))), "{name}: got {result:?}");
        }
    }
}
