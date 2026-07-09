//! S3-compatible object-storage testcontainer for jobmanager integration tests.
//!
//! Kept local to this crate (rather than reusing `icegate_common::testing`) so
//! `icegate-jobmanager` carries no dependency on other IceGate crates. The public
//! API is storage-agnostic (`S3TestContainer`) so the backing image can change
//! without touching call sites.

use std::time::{Duration, Instant};

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::net::TcpStream;

/// Container image backing the S3-compatible test object store.
const RUSTFS_IMAGE: &str = "rustfs/rustfs";
/// Pinned RustFS image tag. Beta software — no GA tag exists as of 2026-07-05.
const RUSTFS_TAG: &str = "1.0.0-beta.8";
/// Root access key (used for both the container and downstream S3 SDK calls).
const STORAGE_ACCESS_KEY: &str = "rustfsadmin";
/// Root secret key (used for both the container and downstream S3 SDK calls).
const STORAGE_SECRET_KEY: &str = "rustfsadmin";

/// Manages an S3-compatible object-storage testcontainer for integration tests.
///
/// The container is automatically cleaned up when this struct is dropped.
pub struct S3TestContainer {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
    username: String,
    password: String,
}

impl S3TestContainer {
    /// Start a new object-storage container and wait until it accepts TCP
    /// connections on the S3 port.
    ///
    /// # Errors
    ///
    /// Returns an error if the container fails to start, the mapped port cannot
    /// be determined, or the store does not become reachable within 10 seconds.
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        tracing::info!("Starting object-storage container...");

        let image = GenericImage::new(RUSTFS_IMAGE, RUSTFS_TAG)
            .with_wait_for(WaitFor::seconds(1))
            .with_exposed_port(9000.tcp())
            .with_env_var("RUSTFS_ACCESS_KEY", STORAGE_ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", STORAGE_SECRET_KEY)
            .with_env_var("RUSTFS_VOLUMES", "/data");

        let container: ContainerAsync<GenericImage> = image.start().await?;

        let port = container.get_host_port_ipv4(9000).await?;
        let endpoint = format!("http://127.0.0.1:{port}");

        tracing::info!("Object-storage container started on {}", endpoint);

        // Wait until the S3 port accepts connections before handing the endpoint
        // to callers, which build their S3 client against it immediately.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match TcpStream::connect(("127.0.0.1", port)).await {
                Ok(_) => break,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(err) => return Err(format!("Object store not reachable: {err}").into()),
            }
        }

        Ok(Self {
            _container: container,
            endpoint,
            username: STORAGE_ACCESS_KEY.to_string(),
            password: STORAGE_SECRET_KEY.to_string(),
        })
    }

    /// Host-mapped S3 API endpoint URL (no trailing slash).
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Root access key.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Root secret key.
    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }
}

// Container cleanup happens automatically when S3TestContainer is dropped.
