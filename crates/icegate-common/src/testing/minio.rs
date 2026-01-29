//! MinIO testcontainer setup for integration tests and benchmarks.

use std::time::{Duration, Instant};

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::net::TcpStream;

/// Manages a `MinIO` testcontainer for integration tests and benchmarks.
///
/// The container is automatically cleaned up when this struct is dropped.
pub struct MinIOContainer {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
    username: String,
    password: String,
}

impl MinIOContainer {
    /// Start a new `MinIO` testcontainer.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The container fails to start
    /// - The mapped port cannot be determined
    /// - `MinIO` does not become reachable within 10 seconds
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        tracing::info!("Starting MinIO container...");

        // Use MinIO release that supports If-None-Match: *
        let minio_image = GenericImage::new("minio/minio", "RELEASE.2025-01-20T14-49-07Z")
            .with_wait_for(WaitFor::seconds(1))
            .with_exposed_port(9000.tcp())
            .with_exposed_port(9001.tcp())
            .with_env_var("MINIO_ROOT_USER", "minioadmin")
            .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
            .with_env_var("MINIO_CONSOLE_ADDRESS", ":9001")
            .with_cmd(["server", "/data"]);

        let container: ContainerAsync<GenericImage> = minio_image.start().await?;

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

        Ok(Self {
            _container: container,
            endpoint,
            username: "minioadmin".to_string(),
            password: "minioadmin".to_string(),
        })
    }

    /// Get the `MinIO` API endpoint URL.
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
}

// Container cleanup happens automatically when MinIOContainer is dropped
