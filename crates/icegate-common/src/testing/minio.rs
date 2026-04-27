//! MinIO testcontainer setup for integration tests and benchmarks.

use std::time::{Duration, Instant};

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::net::TcpStream;

/// `MinIO` image used for all testcontainer-based tests in the workspace.
const MINIO_IMAGE: &str = "minio/minio";
/// `MinIO` image tag — pinned to a release that supports `If-None-Match: *`.
const MINIO_TAG: &str = "RELEASE.2025-01-20T14-49-07Z";
/// `MinIO` root username (used for both the container and downstream S3 SDK calls).
const MINIO_ROOT_USER: &str = "minioadmin";
/// `MinIO` root password (used for both the container and downstream S3 SDK calls).
const MINIO_ROOT_PASSWORD: &str = "minioadmin";

/// Builder for [`MinIOContainer`] that supports optional Docker network and
/// container-name configuration for inter-container DNS scenarios.
#[derive(Debug, Default)]
pub struct MinIOContainerBuilder {
    network: Option<String>,
    container_name: Option<String>,
}

impl MinIOContainerBuilder {
    /// Place the container on the given user-defined Docker network. Required
    /// when other containers need to reach `MinIO` by container name.
    #[must_use]
    pub fn network(mut self, name: impl Into<String>) -> Self {
        self.network = Some(name.into());
        self
    }

    /// Assign a fixed container name. Required when other containers on the
    /// same network need to resolve `MinIO` via DNS.
    #[must_use]
    pub fn container_name(mut self, name: impl Into<String>) -> Self {
        self.container_name = Some(name.into());
        self
    }

    /// Start the configured `MinIO` container and wait until it accepts TCP
    /// connections.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The container fails to start
    /// - The mapped port cannot be determined
    /// - `MinIO` does not become reachable within 10 seconds
    pub async fn start(self) -> Result<MinIOContainer, Box<dyn std::error::Error>> {
        tracing::info!("Starting MinIO container...");

        let mut image = GenericImage::new(MINIO_IMAGE, MINIO_TAG)
            .with_wait_for(WaitFor::seconds(1))
            .with_exposed_port(9000.tcp())
            .with_exposed_port(9001.tcp())
            .with_env_var("MINIO_ROOT_USER", MINIO_ROOT_USER)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
            .with_env_var("MINIO_CONSOLE_ADDRESS", ":9001")
            .with_cmd(["server", "/data"]);

        // `with_network` and `with_container_name` come from `ImageExt` and
        // change the type to `RequestedContainer<GenericImage>`. To keep the
        // method-chained type uniform with the no-options path we apply them
        // immediately before `.start()`.
        if let Some(net) = &self.network {
            image = image.with_network(net.clone());
        }
        if let Some(name) = &self.container_name {
            image = image.with_container_name(name.clone());
        }

        let container: ContainerAsync<GenericImage> = image.start().await?;

        // Get the mapped port for MinIO API
        let port = container.get_host_port_ipv4(9000).await?;
        let endpoint = format!("http://127.0.0.1:{port}");

        tracing::info!("MinIO container started on {}", endpoint);

        // Wait for MinIO to accept connections
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match TcpStream::connect(("127.0.0.1", port)).await {
                Ok(_) => break,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(err) => return Err(format!("MinIO not reachable: {err}").into()),
            }
        }

        Ok(MinIOContainer {
            _container: container,
            endpoint,
            username: MINIO_ROOT_USER.to_string(),
            password: MINIO_ROOT_PASSWORD.to_string(),
            container_name: self.container_name,
        })
    }
}

/// Manages a `MinIO` testcontainer for integration tests and benchmarks.
///
/// The container is automatically cleaned up when this struct is dropped.
pub struct MinIOContainer {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
    username: String,
    password: String,
    container_name: Option<String>,
}

impl MinIOContainer {
    /// Return a builder for configuring the `MinIO` container before starting.
    #[must_use]
    pub fn builder() -> MinIOContainerBuilder {
        MinIOContainerBuilder::default()
    }

    /// Start a new `MinIO` testcontainer with default settings (no shared
    /// network, no fixed container name).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The container fails to start
    /// - The mapped port cannot be determined
    /// - `MinIO` does not become reachable within 10 seconds
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        Self::builder().start().await
    }

    /// Get the host-mapped `MinIO` API endpoint URL (no trailing slash).
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Get the `MinIO` root username.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Get the `MinIO` root password.
    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }

    /// Get the container name if one was assigned via the builder.
    #[must_use]
    pub fn container_name(&self) -> Option<&str> {
        self.container_name.as_deref()
    }

    /// Get the network-internal endpoint for use by sibling containers on the
    /// same Docker network. Returns `None` when the container was not started
    /// with a fixed container name.
    #[must_use]
    pub fn internal_endpoint(&self) -> Option<String> {
        self.container_name.as_ref().map(|name| format!("http://{name}:9000"))
    }
}

// Container cleanup happens automatically when MinIOContainer is dropped
