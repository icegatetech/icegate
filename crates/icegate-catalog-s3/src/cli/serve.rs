//! `CatalogServer` lifecycle implementation.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::CliError;
use crate::api::router;
use crate::{CatalogServerConfig, S3Catalog};

/// Bind and serve the Catalog REST API until SIGINT or SIGTERM requests shutdown.
pub(crate) async fn run(
    config: CatalogServerConfig,
    catalog: Arc<S3Catalog>,
    cancellation: CancellationToken,
) -> Result<(), CliError> {
    let address = config.listen_address()?;
    let api_config = config.to_api_config()?;
    let listener = tokio::net::TcpListener::bind(address).await.map_err(CliError::Bind)?;
    tracing::info!(%address, "Catalog REST server listening");

    let shutdown = cancellation.clone();
    tokio::spawn(async move {
        if let Err(error) = wait_for_shutdown_signal().await {
            tracing::error!(%error, "Catalog shutdown signal handler failed");
        }
        shutdown.cancel();
    });

    axum::serve(listener, router(catalog, api_config))
        .with_graceful_shutdown(async move {
            cancellation.cancelled().await;
            tracing::info!("Catalog REST server shutting down gracefully");
        })
        .await
        .map_err(CliError::Server)
}

async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        signal.recv().await;
        Ok::<(), std::io::Error>(())
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<std::io::Result<()>>();

    tokio::select! {
        result = interrupt => result,
        result = terminate => result,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::time::Duration;

    use super::*;
    use crate::config::{CatalogServerHttpConfig, CatalogServerS3Config};
    use crate::tests::common::make_in_memory_catalog;

    fn server_config(port: u16) -> CatalogServerConfig {
        CatalogServerConfig {
            http: CatalogServerHttpConfig {
                host: "127.0.0.1".to_string(),
                port,
                prefix: None,
                default_page_size: 1,
                max_page_size: 2,
            },
            s3: CatalogServerS3Config {
                bucket: "catalog".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                path_style_access: None,
                access_key_id: None,
                secret_access_key: None,
                warehouse: "warehouse".to_string(),
                codec: "json".to_string(),
            },
        }
    }

    /// The `CancellationToken` is the shutdown contract of `run`: with the
    /// token already cancelled, `run` must finish with `Ok` rather than hang or
    /// error. Bound to port `0` so parallel test runs never collide. Tested
    /// here because the binary's own trigger is a process signal, which a test
    /// must not raise.
    ///
    /// Deliberate coverage gap: the accepting-server -> graceful-shutdown
    /// transition is not deterministically testable at this signature — `run`
    /// binds port `0` internally and never reports the chosen address, so no
    /// readiness probe can precede the cancellation.
    #[tokio::test]
    async fn run_stops_when_started_with_a_cancelled_token() {
        let catalog = Arc::new(make_in_memory_catalog());
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let outcome = tokio::time::timeout(Duration::from_secs(5), run(server_config(0), catalog, cancellation)).await;

        outcome
            .expect("cancelled server must stop within its shutdown budget")
            .expect("graceful shutdown must finish with Ok");
    }

    /// A port that is already owned must fail `run` instead of leaving a
    /// process that reports itself started while another server answers its
    /// traffic. Exercised on the lifecycle function with an in-memory catalog:
    /// no S3 client exists, and the pre-assertions prove the address resolves
    /// to the occupied one and the API config converts, so the bind is the only
    /// step of `run` left that can fail.
    #[tokio::test]
    async fn run_fails_when_the_configured_port_is_already_bound() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("occupy a port");
        let port = occupied.local_addr().expect("occupied address").port();
        let catalog = Arc::new(make_in_memory_catalog());
        let config = server_config(port);
        assert_eq!(
            config.listen_address().expect("listen address"),
            occupied.local_addr().expect("occupied address"),
        );
        config.to_api_config().expect("api config");

        let error = tokio::time::timeout(Duration::from_secs(5), run(config, catalog, CancellationToken::new()))
            .await
            .expect("run must fail promptly on a bind conflict")
            .expect_err("occupied port must fail");

        assert!(matches!(error, CliError::Bind(_)), "{error}");
    }
}
