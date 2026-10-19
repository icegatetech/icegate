//! Server-side listener plumbing shared by the services' network surfaces.

pub mod certificate;
pub mod connection;
pub mod runtime;
pub mod server_listener;
pub mod tls_config;

pub use certificate::{CertificateReload, ServerCertificate};
pub use connection::Connection;
pub use runtime::{ListenerRuntime, start_listener_runtime};
pub use server_listener::{ApplicationProtocol, ServerListener};
pub use tls_config::{DEFAULT_TLS_RELOAD_INTERVAL_SECS, MAX_TLS_RELOAD_INTERVAL_SECS, TlsConfig};
