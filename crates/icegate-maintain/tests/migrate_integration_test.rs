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
    CancellationToken, LOGS_TABLE, PRICES_TABLE,
    catalog::{CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle},
    icegate_table_ident,
    testing::{S3TestContainer, create_s3_bucket},
};
use icegate_maintain::migrate::config::SnapshotExpirationConfig;
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

/// Test that `create_tables` creates all 6 observability tables
#[tokio::test]
async fn test_migrate_create_tables() {
    let (_store, config) = setup_containers().await;

    println!("Creating catalog with config: {:?}", config);

    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");

    println!("Catalog created, calling create_tables...");

    let ops = match create_tables(&catalog, &SnapshotExpirationConfig::default(), false).await {
        Ok(ops) => ops,
        Err(e) => {
            println!("Error creating tables: {:?}", e);
            panic!("Failed to create tables: {e}");
        }
    };

    // Should have created 6 tables: logs, spans, events, metrics, operations, prices
    assert_eq!(ops.len(), 6, "Expected 6 table creation operations");

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
    assert!(table_names.contains(&"prices"));
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
    let ops = create_tables(&catalog, &SnapshotExpirationConfig::default(), true)
        .await
        .expect("Failed to dry-run create tables");

    // Should report 6 operations
    assert_eq!(ops.len(), 6, "Expected 6 table creation operations in dry-run");

    // Verify NO tables were actually created by calling create_tables again
    // This time with dry_run=false, it should still create 6 tables
    let ops_actual = create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("Failed to create tables after dry-run");

    assert_eq!(
        ops_actual.len(),
        6,
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
    let ops_first = create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("Failed to create tables first time");
    assert_eq!(ops_first.len(), 6, "First call should create 6 tables");

    // Second call - should be idempotent
    let ops_second = create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
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
    let create_ops = create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("Failed to create tables");
    assert_eq!(create_ops.len(), 6);

    // Now call upgrade_schemas - should return 0 operations since schemas are
    // current
    let upgrade_ops = upgrade_schemas(&catalog, false).await.expect("Failed to upgrade schemas");

    assert_eq!(
        upgrade_ops.len(),
        0,
        "Expected 0 upgrade operations (schemas are up to date)"
    );
}

/// The retention policy has to survive the round trip through the catalog: it is
/// the table's own properties, not any config file, that every later writer
/// resolves its expiration window from. A window that never reaches
/// `metadata.json` is a feature that silently does nothing.
#[tokio::test]
async fn created_tables_carry_the_configured_retention_policy() {
    let (_store, config) = setup_containers().await;

    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");

    let expiration = SnapshotExpirationConfig {
        enabled: true,
        min_snapshots_to_keep: 7,
        max_snapshot_age_ms: 90_000,
        metadata_previous_versions_max: 14,
    };
    create_tables(&catalog, &expiration, false)
        .await
        .expect("Failed to create tables");

    let logs = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");
    let properties = logs.metadata().properties();

    assert_eq!(properties.get("history.expire.enabled"), Some(&"true".to_string()));
    assert_eq!(
        properties.get("history.expire.min-snapshots-to-keep"),
        Some(&"7".to_string())
    );
    assert_eq!(
        properties.get("history.expire.max-snapshot-age-ms"),
        Some(&"90000".to_string())
    );
    assert_eq!(
        properties.get("write.metadata.previous-versions-max"),
        Some(&"14".to_string())
    );
    // The WAL offset lives in the snapshot summary under this key; expiration
    // must keep its most recent carrier — and the chain to it — reachable, or
    // the Shifter resumes from 0 and re-commits the whole queue.
    assert_eq!(
        properties.get("history.expire.preserve-summary-property"),
        Some(&"icegate.queue.offset".to_string())
    );
    // Pre-existing table properties must not be lost to the new ones.
    assert_eq!(properties.get("write.format.default"), Some(&"parquet".to_string()));

    let prices = catalog
        .load_table(&icegate_table_ident(PRICES_TABLE))
        .await
        .expect("load prices table");
    assert!(
        !prices
            .metadata()
            .properties()
            .contains_key("history.expire.preserve-summary-property"),
        "prices is written by the pricing crawler, so no snapshot of it carries a WAL offset"
    );
}

/// Turning expiration off in the config must reach the table as an explicit
/// `false`, not as an absent key: a writer reads the property, and only an
/// explicit value distinguishes a table opted out from one created before the
/// policy existed.
#[tokio::test]
async fn disabled_expiration_reaches_the_table_as_an_explicit_false() {
    let (_store, config) = setup_containers().await;

    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");

    let expiration = SnapshotExpirationConfig {
        enabled: false,
        ..SnapshotExpirationConfig::default()
    };
    create_tables(&catalog, &expiration, false)
        .await
        .expect("Failed to create tables");

    let logs = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");

    assert_eq!(
        logs.metadata().properties().get("history.expire.enabled"),
        Some(&"false".to_string())
    );
}

/// The policy is written once, at creation: `migrate create` skips a table that
/// already exists, properties included. An operator who widens the window in the
/// config and re-runs the command gets nothing — there is no backfill — and the
/// tables keep the policy they were created with. That is what the deployment
/// contract of `migrate/README.md` promises, so it is asserted rather than left
/// to the skip path's `continue`.
#[tokio::test]
async fn a_second_create_leaves_an_existing_table_on_its_original_policy() {
    let (_store, config) = setup_containers().await;

    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");

    let original = SnapshotExpirationConfig {
        enabled: true,
        min_snapshots_to_keep: 7,
        max_snapshot_age_ms: 90_000,
        metadata_previous_versions_max: 14,
    };
    create_tables(&catalog, &original, false)
        .await
        .expect("Failed to create tables");

    // Every field differs, so any re-stamp shows up whichever one it touches.
    let widened = SnapshotExpirationConfig {
        enabled: false,
        min_snapshots_to_keep: 21,
        max_snapshot_age_ms: 600_000,
        metadata_previous_versions_max: 42,
    };
    let ops = create_tables(&catalog, &widened, false).await.expect("Failed to re-run create");
    assert!(ops.is_empty(), "every table already exists, so nothing is created");

    let logs = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");
    let properties = logs.metadata().properties();

    assert_eq!(properties.get("history.expire.enabled"), Some(&"true".to_string()));
    assert_eq!(
        properties.get("history.expire.min-snapshots-to-keep"),
        Some(&"7".to_string())
    );
    assert_eq!(
        properties.get("history.expire.max-snapshot-age-ms"),
        Some(&"90000".to_string())
    );
    assert_eq!(
        properties.get("write.metadata.previous-versions-max"),
        Some(&"14".to_string())
    );
    assert_eq!(
        properties.get("history.expire.preserve-summary-property"),
        Some(&"icegate.queue.offset".to_string())
    );
}
