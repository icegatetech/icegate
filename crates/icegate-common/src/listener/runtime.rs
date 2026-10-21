//! What every listener of one process shares: the shutdown signal, and whether to speak TLS.

use std::sync::Arc;

use crate::CancellationToken;
use crate::error::{CommonError, Result};
use crate::listener::{ServerCertificate, TlsConfig};

/// The shutdown token and the optional server certificate a process binds its listeners
/// with.
///
/// Meant to be cloned into every listener of the process. The clones share one token and
/// one certificate, so cancelling the token reaches every listener, and a reload of the
/// certificate is presented by the next handshake on every TLS listener.
///
/// The token is the shutdown signal: each server stops on it through the graceful shutdown
/// it is served with, and a TLS listener also stops accepting on it. Without a certificate
/// every listener serves plaintext.
#[derive(Debug, Clone)]
pub struct ListenerRuntime {
    cancel_token: CancellationToken,
    certificate: Option<Arc<ServerCertificate>>,
}

impl ListenerRuntime {
    /// Listeners that stop on `cancel_token` and serve TLS with `certificate` when one is
    /// given, plaintext otherwise.
    pub const fn new(cancel_token: CancellationToken, certificate: Option<Arc<ServerCertificate>>) -> Self {
        Self {
            cancel_token,
            certificate,
        }
    }

    /// Listeners that stop on `cancel_token` and serve plaintext.
    pub const fn plaintext(cancel_token: CancellationToken) -> Self {
        Self::new(cancel_token, None)
    }

    /// The shutdown signal of every listener bound with this runtime.
    pub const fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// The certificate a TLS listener presents; `None` means plaintext.
    pub(crate) fn certificate(&self) -> Option<&ServerCertificate> {
        self.certificate.as_deref()
    }
}

/// Build the [`ListenerRuntime`] every listener of one process shares.
///
/// With a `tls` block the certificate is loaded here, and one task re-reads its files until
/// `cancel_token` is cancelled. That is the only such task of the process, however many
/// listeners serve TLS: the reload metrics carry no label that could tell two of them apart.
/// Without a block every listener serves plaintext.
///
/// Call it before the process starts anything else that costs something to stop, so that a
/// pair that cannot be loaded ends the process before any of it is running. The reload task
/// runs on the Tokio runtime current at the call, so call it from within one when `tls` is
/// `Some`.
///
/// # Errors
///
/// [`CommonError::Config`](crate::Error::Config) when `tls` fails [`TlsConfig::validate`],
/// and [`CommonError::Tls`](crate::Error::Tls) when the files cannot be loaded as a matching
/// pair, or when `tls` is `Some` and no Tokio runtime is current. The caller stops the
/// process: serving plaintext where TLS was asked for would be worse than not serving.
pub fn start_listener_runtime(tls: Option<&TlsConfig>, cancel_token: &CancellationToken) -> Result<ListenerRuntime> {
    let Some(tls) = tls else {
        return Ok(ListenerRuntime::plaintext(cancel_token.clone()));
    };

    let certificate = Arc::new(ServerCertificate::load(tls)?);
    // `tokio::spawn` panics without a runtime. The lookup comes after the load so a
    // pair that cannot be loaded is reported as that, whatever thread the caller is on.
    let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
        CommonError::Tls(format!(
            "cannot start the certificate reload task: no Tokio runtime is running: {error}"
        ))
    })?;
    runtime.spawn(Arc::clone(&certificate).watch_files(cancel_token.clone()));
    tracing::info!(
        certificate_path = %tls.certificate_path.display(),
        reload_interval_secs = tls.reload_interval_secs,
        "TLS enabled on every listener"
    );
    Ok(ListenerRuntime::new(cancel_token.clone(), Some(certificate)))
}
