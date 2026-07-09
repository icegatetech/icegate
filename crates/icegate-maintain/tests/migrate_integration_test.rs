//! Integration tests for maintain migrate operations.
//!
//! Runs an object-storage container via testcontainers and builds the S3 catalog in-process
//! (matching the default `backend: !s3` config), so no external catalog service
//! is required.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation
)]

use std::collections::HashMap;

use icegate_common::{
    CancellationToken,
    catalog::{CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle},
    testing::{S3TestContainer, create_s3_bucket},
};
use icegate_maintain::migrate::operations::{MigrationOperation, create_tables, upgrade_schemas};

/// Object-storage bucket used as the Iceberg warehouse root for the catalog under test.
const BUCKET_NAME: &str = "warehouse";

/// Start object storage and build an S3-catalog config pointing at it (mirrors the
/// default `backend: !s3` config and `builder::tests::test_catalog_config`).
async fn setup_containers() -> (S3TestContainer, CatalogConfig) {
    let store = S3TestContainer::start()
        .await
        .expect("Failed to start object-storage container");
    create_s3_bucket(store.endpoint(), BUCKET_NAME)
        .await
        .expect("Failed to create S3 bucket");

    let config = CatalogConfig {
        backend: CatalogBackend::S3 {
            warehouse: "catalog".to_string(),
        },
        warehouse: format!("s3://{BUCKET_NAME}/"),
        properties: HashMap::from([
            ("bucket".to_string(), BUCKET_NAME.to_string()),
            ("region".to_string(), "us-east-1".to_string()),
            ("endpoint".to_string(), store.endpoint().to_string()),
            ("access_key_id".to_string(), store.username().to_string()),
            ("secret_access_key".to_string(), store.password().to_string()),
        ]),
        cache: None,
    };

    (store, config)
}

/// Test that `create_tables` creates all 5 observability tables
#[tokio::test]
async fn test_migrate_create_tables() {
    let (_store, config) = setup_containers().await;

    println!("Creating catalog with config: {:?}", config);

    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");

    println!("Catalog created, calling create_tables...");

    let ops = match create_tables(&catalog, false).await {
        Ok(ops) => ops,
        Err(e) => {
            println!("Error creating tables: {:?}", e);
            panic!("Failed to create tables: {e}");
        }
    };

    // Should have created 5 tables: logs, spans, events, metrics, operations
    assert_eq!(ops.len(), 5, "Expected 5 table creation operations");

    // Verify table names
    let table_names: Vec<&str> = ops
        .iter()
        .map(|op| match op {
            MigrationOperation::Create { table_name } => table_name.as_str(),
            MigrationOperation::Upgrade { .. } => {
                panic!("Expected Create operation")
            }
        })
        .collect();

    assert!(table_names.contains(&"logs"));
    assert!(table_names.contains(&"spans"));
    assert!(table_names.contains(&"events"));
    assert!(table_names.contains(&"metrics"));
    assert!(table_names.contains(&"operations"));
}

/// Test that `create_tables` with `dry_run=true` returns operations but doesn't
/// create tables
#[tokio::test]
async fn test_migrate_create_tables_dry_run() {
    let (_store, config) = setup_containers().await;

    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");

    // First call with dry_run=true
    let ops = create_tables(&catalog, true).await.expect("Failed to dry-run create tables");

    // Should report 5 operations
    assert_eq!(ops.len(), 5, "Expected 5 table creation operations in dry-run");

    // Verify NO tables were actually created by calling create_tables again
    // This time with dry_run=false, it should still create 5 tables
    let ops_actual = create_tables(&catalog, false)
        .await
        .expect("Failed to create tables after dry-run");

    assert_eq!(
        ops_actual.len(),
        5,
        "Tables should not have been created during dry-run"
    );
}

/// Test that `create_tables` is idempotent - calling twice returns 0 operations
#[tokio::test]
async fn test_migrate_create_tables_idempotent() {
    let (_store, config) = setup_containers().await;

    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");

    // First call - creates tables
    let ops_first = create_tables(&catalog, false)
        .await
        .expect("Failed to create tables first time");
    assert_eq!(ops_first.len(), 5, "First call should create 5 tables");

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
    let (_store, config) = setup_containers().await;

    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");

    // First create the tables
    let create_ops = create_tables(&catalog, false).await.expect("Failed to create tables");
    assert_eq!(create_ops.len(), 5);

    // Now call upgrade_schemas - should return 0 operations since schemas are
    // current
    let upgrade_ops = upgrade_schemas(&catalog, false).await.expect("Failed to upgrade schemas");

    assert_eq!(
        upgrade_ops.len(),
        0,
        "Expected 0 upgrade operations (schemas are up to date)"
    );
}
