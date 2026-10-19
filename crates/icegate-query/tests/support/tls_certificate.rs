//! A self-signed certificate for the TLS cases of the integration suites.
//!
//! Shared by the test binaries through `#[path]`, so each of them compiles its own copy.
#![allow(clippy::expect_used)]

use std::{fs, sync::Arc};

use icegate_common::{DEFAULT_TLS_RELOAD_INTERVAL_SECS, ServerCertificate, TlsConfig};
use rcgen::{CertificateParams, KeyPair};
use tempfile::TempDir;

/// The name the certificate is issued for, which clients verify the server against.
pub const CERTIFICATE_HOST: &str = "localhost";

/// A self-signed certificate for [`CERTIFICATE_HOST`], written to disk and loaded the way the
/// binary loads one, together with the PEM that clients trust.
pub struct TestCertificate {
    /// Owns the files the certificate was loaded from.
    _directory: TempDir,
    server_certificate: Arc<ServerCertificate>,
    pem: String,
}

impl TestCertificate {
    pub fn generate() -> Self {
        let key_pair = KeyPair::generate().expect("generate key pair");
        let certificate = CertificateParams::new(vec![CERTIFICATE_HOST.to_string()])
            .expect("certificate params")
            .self_signed(&key_pair)
            .expect("self-sign certificate");

        let directory = TempDir::new().expect("temp directory");
        let certificate_path = directory.path().join("tls.crt");
        let private_key_path = directory.path().join("tls.key");
        fs::write(&certificate_path, certificate.pem()).expect("write certificate");
        fs::write(&private_key_path, key_pair.serialize_pem()).expect("write private key");

        let server_certificate = ServerCertificate::load(&TlsConfig {
            certificate_path,
            private_key_path,
            reload_interval_secs: DEFAULT_TLS_RELOAD_INTERVAL_SECS,
        })
        .expect("load the generated pair");

        Self {
            _directory: directory,
            server_certificate: Arc::new(server_certificate),
            pem: certificate.pem(),
        }
    }

    /// The certificate as a server presents it.
    pub fn server_certificate(&self) -> Arc<ServerCertificate> {
        Arc::clone(&self.server_certificate)
    }

    /// The certificate a client must trust to accept it.
    pub fn pem(&self) -> &str {
        &self.pem
    }
}
