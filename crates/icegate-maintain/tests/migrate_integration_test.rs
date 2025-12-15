//! Integration tests for maintain migrate operations
//!
//! Uses testcontainers to run `MinIO` and Iceberg REST catalog containers.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation
)] // Test code can use unwrap/expect and println

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use icegate_common::catalog::{CatalogBackend, CatalogBuilder, CatalogConfig};
use icegate_maintain::migrate::operations::{create_tables, upgrade_schemas, MigrationOperation};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use testcontainers_modules::minio::MinIO;

/// Counter for generating unique container names
static CONTAINER_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared network name for container-to-container communication
const NETWORK_NAME: &str = "icegate-test-net";

/// `MinIO` credentials (default)
const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const BUCKET_NAME: &str = "warehouse";

/// Setup `MinIO` and Iceberg REST containers
async fn setup_containers() -> (ContainerAsync<MinIO>, ContainerAsync<GenericImage>, CatalogConfig) {
    // Start MinIO container on shared network with a unique name
    // Using an atomic counter to generate unique names across parallel tests
    let test_id = CONTAINER_COUNTER.fetch_add(1, Ordering::SeqCst);
    let minio_container_name = format!("minio-{}-{}", std::process::id(), test_id);
    println!("Starting MinIO container (name: {})...", minio_container_name);
    // Pre-pull the image to avoid timeout during startup
    let minio_image = MinIO::default()
        .with_network(NETWORK_NAME)
        .with_container_name(&minio_container_name)
        .pull_image()
        .await
        .expect("Failed to pull MinIO image");

    let minio = minio_image.start().await.expect("Failed to start MinIO container");

    let minio_port = minio.get_host_port_ipv4(9000).await.expect("Failed to get MinIO port");
    println!(
        "MinIO started on port {} (container: {}:9000 on network)",
        minio_port, minio_container_name
    );

    // Create bucket using AWS SDK
    println!("Creating S3 bucket...");
    create_s3_bucket(minio_port).await;
    println!("S3 bucket created");

    // Start Nessie catalog container (Iceberg REST compatible) on same network as
    // MinIO
    println!("Starting Nessie catalog container...");
    // Use container name to reach MinIO via Docker network
    let s3_internal_endpoint = format!("http://{}:9000/", minio_container_name);
    // External endpoint for clients on the host
    let s3_external_endpoint = format!("http://127.0.0.1:{minio_port}/");
    println!("Nessie will connect to S3 at: {} (internal)", s3_internal_endpoint);
    println!("Clients will access S3 at: {} (external)", s3_external_endpoint);

    // Nessie requires catalog configuration to enable Iceberg REST API
    // Use dot notation for Quarkus configuration (supported by Nessie)
    // Use quay.io registry with specific version that has Iceberg REST catalog
    // Note: with_wait_for must be called before with_network to stay on
    // GenericImage Pre-pull the image to avoid timeout during startup
    let nessie_image = GenericImage::new("quay.io/projectnessie/nessie", "0.105.7")
        .with_exposed_port(19120.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Listening on: http://0.0.0.0:19120"))
        // Nessie takes time to initialize S3 catalog, increase startup timeout from default 60s
        .with_startup_timeout(Duration::from_secs(120))
        .with_network(NETWORK_NAME)
        // Disable authentication for testing
        .with_env_var("nessie.server.authentication.enabled", "false")
        // Configure default warehouse
        .with_env_var("nessie.catalog.default-warehouse", "warehouse")
        // Configure warehouse location in S3
        .with_env_var(
            "nessie.catalog.warehouses.warehouse.location",
            format!("s3://{BUCKET_NAME}/"),
        )
        // Configure S3 endpoint (internal for Nessie, external for clients)
        .with_env_var(
            "nessie.catalog.service.s3.default-options.endpoint",
            &s3_internal_endpoint,
        )
        .with_env_var(
            "nessie.catalog.service.s3.default-options.external-endpoint",
            &s3_external_endpoint,
        )
        .with_env_var(
            "nessie.catalog.service.s3.default-options.path-style-access",
            "true",
        )
        .with_env_var(
            "nessie.catalog.service.s3.default-options.region",
            "us-east-1",
        )
        // Configure S3 credentials via secret reference
        .with_env_var(
            "nessie.catalog.service.s3.default-options.access-key",
            "urn:nessie-secret:quarkus:nessie.catalog.secrets.access-key",
        )
        .with_env_var("nessie.catalog.secrets.access-key.name", MINIO_ACCESS_KEY)
        .with_env_var("nessie.catalog.secrets.access-key.secret", MINIO_SECRET_KEY)
        // Disable AWS SDK v2 default checksum calculation for MinIO compatibility
        // See: https://github.com/aws/aws-sdk-java-v2/discussions/5802
        .with_env_var("AWS_REQUEST_CHECKSUM_CALCULATION", "WHEN_REQUIRED")
        .with_env_var("AWS_RESPONSE_CHECKSUM_VALIDATION", "WHEN_REQUIRED")
        .pull_image()
        .await
        .expect("Failed to pull Nessie image");

    let iceberg = nessie_image.start().await.expect("Failed to start Nessie container");

    let nessie_port = iceberg.get_host_port_ipv4(19120).await.expect("Failed to get Nessie port");
    println!("Nessie catalog started on port {}", nessie_port);

    // Nessie Iceberg REST API base URL (branch is specified via prefix property)
    let iceberg_base = format!("http://127.0.0.1:{nessie_port}/iceberg");

    // Build catalog config - Nessie REST API is at /iceberg with prefix=main for
    // branch
    let mut properties = HashMap::new();
    properties.insert("prefix".to_string(), "main".to_string());

    let config = CatalogConfig {
        backend: CatalogBackend::Rest {
            uri: iceberg_base,
        },
        warehouse: format!("s3://{BUCKET_NAME}/"),
        properties,
    };

    (minio, iceberg, config)
}

/// Create an S3 bucket in `MinIO` using AWS SDK
async fn create_s3_bucket(minio_port: u16) {
    let creds = Credentials::new(MINIO_ACCESS_KEY, MINIO_SECRET_KEY, None, None, "test");

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(format!("http://127.0.0.1:{minio_port}"))
        .credentials_provider(creds)
        .load()
        .await;

    let s3_client = aws_sdk_s3::Client::new(&config);

    s3_client
        .create_bucket()
        .bucket(BUCKET_NAME)
        .send()
        .await
        .expect("Failed to create S3 bucket");
}

/// Test that `create_tables` creates all 4 observability tables
#[tokio::test]
async fn test_migrate_create_tables() {
    let (_minio, _iceberg, config) = setup_containers().await;

    println!("Creating catalog with config: {:?}", config);

    let catalog = CatalogBuilder::from_config(&config).await.expect("Failed to create catalog");

    println!("Catalog created, calling create_tables...");

    let ops = match create_tables(&catalog, false).await {
        Ok(ops) => ops,
        Err(e) => {
            println!("Error creating tables: {:?}", e);
            panic!("Failed to create tables: {e}");
        },
    };

    // Should have created 4 tables: logs, spans, events, metrics
    assert_eq!(ops.len(), 4, "Expected 4 table creation operations");

    // Verify table names
    let table_names: Vec<&str> = ops
        .iter()
        .map(|op| match op {
            MigrationOperation::Create {
                table_name,
            } => table_name.as_str(),
            MigrationOperation::Upgrade {
                ..
            } => {
                panic!("Expected Create operation")
            },
        })
        .collect();

    assert!(table_names.contains(&"logs"));
    assert!(table_names.contains(&"spans"));
    assert!(table_names.contains(&"events"));
    assert!(table_names.contains(&"metrics"));
}

/// Test that `create_tables` with `dry_run=true` returns operations but doesn't
/// create tables
#[tokio::test]
async fn test_migrate_create_tables_dry_run() {
    let (_minio, _iceberg, config) = setup_containers().await;

    let catalog = CatalogBuilder::from_config(&config).await.expect("Failed to create catalog");

    // First call with dry_run=true
    let ops = create_tables(&catalog, true).await.expect("Failed to dry-run create tables");

    // Should report 4 operations
    assert_eq!(ops.len(), 4, "Expected 4 table creation operations in dry-run");

    // Verify NO tables were actually created by calling create_tables again
    // This time with dry_run=false, it should still create 4 tables
    let ops_actual = create_tables(&catalog, false)
        .await
        .expect("Failed to create tables after dry-run");

    assert_eq!(
        ops_actual.len(),
        4,
        "Tables should not have been created during dry-run"
    );
}

/// Test that `create_tables` is idempotent - calling twice returns 0 operations
#[tokio::test]
async fn test_migrate_create_tables_idempotent() {
    let (_minio, _iceberg, config) = setup_containers().await;

    let catalog = CatalogBuilder::from_config(&config).await.expect("Failed to create catalog");

    // First call - creates tables
    let ops_first = create_tables(&catalog, false)
        .await
        .expect("Failed to create tables first time");
    assert_eq!(ops_first.len(), 4, "First call should create 4 tables");

    // Second call - should be idempotent
    let ops_second = create_tables(&catalog, false)
        .await
        .expect("Failed to create tables second time");
    assert_eq!(
        ops_second.len(),
        0,
        "Second call should return 0 operations (tables already exist)"
    );
}

/// Test that `upgrade_schemas` returns 0 operations when schemas are up to date
#[tokio::test]
async fn test_migrate_upgrade_schemas() {
    let (_minio, _iceberg, config) = setup_containers().await;

    let catalog = CatalogBuilder::from_config(&config).await.expect("Failed to create catalog");

    // First create the tables
    let create_ops = create_tables(&catalog, false).await.expect("Failed to create tables");
    assert_eq!(create_ops.len(), 4);

    // Now call upgrade_schemas - should return 0 operations since schemas are
    // current
    let upgrade_ops = upgrade_schemas(&catalog, false).await.expect("Failed to upgrade schemas");

    assert_eq!(
        upgrade_ops.len(),
        0,
        "Expected 0 upgrade operations (schemas are up to date)"
    );
}
