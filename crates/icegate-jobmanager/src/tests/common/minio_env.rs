use std::time::{Duration, Instant};

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::net::TcpStream;

/// MinIOEnv manages a MinIO testcontainer for integration tests
pub struct MinIOEnv {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
    username: String,
    password: String,
}

impl MinIOEnv {
    /// Create a new MinIO test environment
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        tracing::info!("Starting MinIO container...");

        // Release older 2024-01-16 doesn't support If-None-Match: *
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
        let endpoint = format!("http://127.0.0.1:{}", port);

        tracing::info!("MinIO container started on {}", endpoint);

        // Wait for MinIO to accept connections
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match TcpStream::connect(("127.0.0.1", port)).await {
                Ok(_) => break,
                Err(_) if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(err) => return Err(format!("MinIO not reachable: {}", err).into()),
            }
        }

        Ok(Self {
            _container: container,
            endpoint,
            username: "minioadmin".to_string(),
            password: "minioadmin".to_string(),
        })
    }

    /// Get the MinIO API endpoint
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Get the MinIO username
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Get the MinIO password
    pub fn password(&self) -> &str {
        &self.password
    }
}

// Container cleanup happens automatically when MinIOEnv is dropped
