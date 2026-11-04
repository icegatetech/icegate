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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, StringArray, TimestampMicrosecondArray, new_null_array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use async_trait::async_trait;
use chrono::DateTime;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use icegate_common::{
    CancellationToken, EVENTS_TABLE, ICEGATE_NAMESPACE, LOGS_TABLE, METRICS_TABLE, OPERATIONS_TABLE, PRICES_TABLE,
    SPANS_TABLE, WAL_OFFSET_PROPERTY,
    catalog::{CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle},
    iceberg_write::{WriteConfig, write_record_batches_to_parquet},
    icegate_table_ident, resolve_committed_offset,
    schema::{
        COL_BODY, COL_KIND, COL_MAX_WEEK_MINUTE, COL_MIN_WEEK_MINUTE, events_schema, logs_schema, metrics_schema,
        operations_schema, prices_schema, spans_schema,
    },
    testing::{S3TestContainer, create_s3_bucket},
};
use icegate_maintain::error::MaintainError;
use icegate_maintain::migrate::config::SnapshotExpirationConfig;
use icegate_maintain::migrate::operations::{MigrationOperation, create_tables, upgrade_schemas};
use icegate_maintain::pricing::read::read_live_rates;
use icegate_maintain::pricing::source::{RateObservation, ValidFromSource};
use icegate_maintain::pricing::writer::{append_prices, build_price_batch};
use uuid::Uuid;

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
            MigrationOperation::Upgrade { .. } | MigrationOperation::ManualMigrationRequired { .. } => {
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

/// An unscoped rate row.
fn price_row() -> RateObservation {
    RateObservation {
        provider: "openrouter".to_string(),
        model: "deepseek/deepseek-v4.1-flash-1017".to_string(),
        canonical_id: Some("deepseek/deepseek-v4.1-flash-1017".to_string()),
        service_tier: "standard".to_string(),
        region: "global".to_string(),
        min_input_tokens: 0,
        max_input_tokens: None,
        min_week_minute: None,
        max_week_minute: None,
        valid_from: DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp"),
        valid_from_source: ValidFromSource::Observed,
        input_usd_per_1m: Some(0.3),
        output_usd_per_1m: Some(1.2),
        cache_read_usd_per_1m: None,
        cache_write_usd_per_1m: None,
        reasoning_usd_per_1m: None,
        request_usd: None,
        image_input_usd_per_unit: None,
        image_output_usd_per_unit: None,
        audio_input_usd_per_second: None,
        audio_output_usd_per_second: None,
        currency: "USD".to_string(),
        source: "openrouter".to_string(),
        source_url: None,
    }
}

/// Create `prices` in its layout before the time-of-day columns: today's
/// schema without the two trailing week-minute columns. Returns the table and
/// how many columns that layout has.
async fn create_previous_prices_table(catalog: &Arc<dyn Catalog>) -> (Table, usize) {
    let target = prices_schema().expect("prices schema builds");
    let previous_fields: Vec<_> = target
        .as_struct()
        .fields()
        .iter()
        .filter(|f| f.name != COL_MIN_WEEK_MINUTE && f.name != COL_MAX_WEEK_MINUTE)
        .cloned()
        .collect();
    let previous_width = previous_fields.len();
    assert_eq!(previous_width + 2, target.as_struct().fields().len());
    let previous = Schema::builder()
        .with_fields(previous_fields)
        .build()
        .expect("previous prices schema builds");

    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace created");
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder().name(PRICES_TABLE.to_string()).schema(previous).build(),
        )
        .await
        .expect("previous prices table created");
    (table, previous_width)
}

/// A `prices` table created before the time-of-day columns existed gains them
/// in place on the next `migrate create` — the one command every deployment's
/// migrate hook runs — without losing a row or a key: its rows read back
/// unscoped, a time-scoped row then appends beside them, and a second run
/// finds nothing left to do.
#[tokio::test]
async fn create_adds_the_time_of_day_columns_to_an_existing_prices_table() {
    let (_store, config) = setup_containers().await;
    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");
    let (table, previous_width) = create_previous_prices_table(&catalog).await;

    // One row written in the previous layout, as the rc12 crawler wrote it.
    let unscoped = price_row();
    let batch = build_price_batch(std::slice::from_ref(&unscoped)).expect("batch builds");
    let previous_batch = batch
        .project(&(0..previous_width).collect::<Vec<_>>())
        .expect("previous columns are a prefix");
    let write_config = WriteConfig {
        row_group_size: 1_000,
        data_page_size_limit_bytes: 1024 * 1024,
        max_file_size_bytes: 16 * 1024 * 1024,
        bloom_filter_columns: &[],
        column_encodings: &[],
    };
    let written = write_record_batches_to_parquet(
        table.clone(),
        write_config,
        futures::stream::once(async { Ok(previous_batch) }).boxed(),
        &CancellationToken::new(),
    )
    .await
    .expect("previous-layout row written");
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(written.data_files)
        .apply(tx)
        .expect("append applies");
    tx.commit(catalog.as_ref()).await.expect("append commits");

    let ops = create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("create evolves prices");
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOperation::Upgrade { table_name } if table_name == PRICES_TABLE)),
        "prices must be evolved, not skipped: {ops:?}"
    );
    upgrade_schemas(&catalog, false)
        .await
        .expect("no schema drift is left on any table");

    // A scan projects the current snapshot's schema, not the table's, so a
    // schema change with no snapshot behind it leaves every reader on the
    // old columns until something next appends.
    let evolved = catalog
        .load_table(&icegate_table_ident(PRICES_TABLE))
        .await
        .expect("evolved table loads");
    let metadata = evolved.metadata();
    assert_eq!(
        metadata
            .current_snapshot()
            .expect("the earlier append is still current")
            .schema_id(),
        Some(metadata.current_schema_id()),
        "the current snapshot must carry the evolved schema"
    );

    let live = read_live_rates(&catalog).await.expect("evolved table reads");
    let unscoped_live = live.get(&unscoped.key()).expect("the earlier row keeps its key");
    assert_eq!(
        (unscoped_live.min_week_minute, unscoped_live.max_week_minute),
        (None, None)
    );

    let scoped = RateObservation {
        min_week_minute: Some(60),
        max_week_minute: Some(240),
        ..price_row()
    };
    append_prices(&catalog, std::slice::from_ref(&scoped))
        .await
        .expect("the evolved table takes a time-scoped row");
    let live = read_live_rates(&catalog).await.expect("evolved table reads");
    let scoped_live = live.get(&scoped.key()).expect("the time-scoped row is live");
    assert_eq!(
        (scoped_live.min_week_minute, scoped_live.max_week_minute),
        (Some(60), Some(240))
    );
    assert!(live.get(&unscoped.key()).is_some(), "the earlier row is still live");

    let again = create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("second create runs");
    assert!(again.is_empty(), "nothing left to create or evolve: {again:?}");
}

/// A `prices` table migrated before its first crawl still comes out with a
/// snapshot carrying the evolved schema. Without one, the first append could
/// race the migration onto the previous schema, and a later `create` would see
/// a current schema and add nothing to correct it.
#[tokio::test]
async fn create_stamps_the_evolved_schema_on_a_prices_table_with_no_snapshot() {
    let (_store, config) = setup_containers().await;
    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");
    let (table, _) = create_previous_prices_table(&catalog).await;
    assert!(
        table.metadata().current_snapshot().is_none(),
        "the table starts with no snapshot"
    );

    create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("create evolves prices");

    let evolved = catalog
        .load_table(&icegate_table_ident(PRICES_TABLE))
        .await
        .expect("evolved table loads");
    let metadata = evolved.metadata();
    assert_eq!(
        metadata
            .current_snapshot()
            .expect("the migration commits a snapshot")
            .schema_id(),
        Some(metadata.current_schema_id()),
        "the current snapshot must carry the evolved schema"
    );
    assert!(
        read_live_rates(&catalog).await.expect("evolved table reads").is_empty(),
        "the stamp adds no rows"
    );
}

/// Every table the schema module defines, with its code schema.
fn code_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        (LOGS_TABLE, logs_schema().expect("logs schema")),
        (SPANS_TABLE, spans_schema().expect("spans schema")),
        (EVENTS_TABLE, events_schema().expect("events schema")),
        (METRICS_TABLE, metrics_schema().expect("metrics schema")),
        (OPERATIONS_TABLE, operations_schema().expect("operations schema")),
        (PRICES_TABLE, prices_schema().expect("prices schema")),
    ]
}

/// `schema` without its trailing run of optional top-level columns: the layout of a table created
/// before those columns were added. Returns the layout and the names of the columns it drops.
fn schema_before_trailing_optional_columns(schema: &Schema) -> (Schema, Vec<String>) {
    let fields = schema.as_struct().fields();
    let kept_len = fields.iter().rposition(|field| field.required).map_or(0, |index| index + 1);
    let (kept, dropped) = fields.split_at(kept_len);
    let previous = Schema::builder()
        .with_fields(kept.iter().cloned())
        .build()
        .expect("previous schema builds");
    (previous, dropped.iter().map(|field| field.name.clone()).collect())
}

/// Commit to `table` one snapshot holding `rows`, if any, and recording `offset`, if any, as the
/// committed WAL offset. An offset stands for a Shifter commit, its absence for a pricing crawler's.
async fn commit_fixture_snapshot(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    rows: Option<RecordBatch>,
    offset: Option<u64>,
) {
    let data_files = match rows {
        Some(batch) => {
            let write_config = WriteConfig {
                row_group_size: 1_000,
                data_page_size_limit_bytes: 1024 * 1024,
                max_file_size_bytes: 16 * 1024 * 1024,
                bloom_filter_columns: &[],
                column_encodings: &[],
            };
            write_record_batches_to_parquet(
                table.clone(),
                write_config,
                futures::stream::once(async { Ok(batch) }).boxed(),
                &CancellationToken::new(),
            )
            .await
            .expect("fixture rows written")
            .data_files
        }
        None => Vec::new(),
    };
    let tx = Transaction::new(table);
    let properties = offset
        .map(|offset| HashMap::from([(WAL_OFFSET_PROPERTY.to_string(), offset.to_string())]))
        .unwrap_or_default();
    tx.fast_append()
        .set_snapshot_properties(properties)
        .add_data_files(data_files)
        .apply(tx)
        .expect("fixture snapshot applies")
        .commit(catalog.as_ref())
        .await
        .expect("fixture snapshot commits");
}

/// Two rows in `schema`, distinct in every required column; optional columns are NULL.
fn two_distinct_rows(schema: &SchemaRef) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .map(|field| -> ArrayRef {
            if field.is_nullable() {
                return new_null_array(field.data_type(), 2);
            }
            match field.data_type() {
                DataType::Utf8 => Arc::new(StringArray::from(vec![
                    format!("{}-0", field.name()),
                    format!("{}-1", field.name()),
                ])),
                DataType::Int64 => Arc::new(Int64Array::from(vec![0, 1])),
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => Arc::new(
                    TimestampMicrosecondArray::from(vec![1_700_000_000_000_000, 1_700_000_000_000_001])
                        .with_timezone_opt(timezone.clone()),
                ),
                DataType::FixedSizeBinary(width) => {
                    let width = usize::try_from(*width).expect("a fixed-size width is non-negative");
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter((0..2u8).map(|row| vec![row; width]))
                            .expect("fixed-size binary column builds"),
                    )
                }
                other => panic!("no fixture value for required column {} of type {other}", field.name()),
            }
        })
        .collect();
    RecordBatch::try_new(Arc::clone(schema), columns).expect("fixture rows build")
}

/// The rows of `batches` over `columns`, in a canonical order.
fn canonical_rows(converter: &RowConverter, batches: &[RecordBatch], columns: &[String]) -> Vec<OwnedRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let arrays: Vec<ArrayRef> = columns
            .iter()
            .map(|column| Arc::clone(batch.column_by_name(column).expect("column is read")))
            .collect();
        let batch_rows = converter.convert_columns(&arrays).expect("rows convert");
        rows.extend(batch_rows.iter().map(|row| row.owned()));
    }
    rows.sort();
    rows
}

/// Every row of `table`'s current snapshot.
async fn scan_rows(table: &Table) -> Vec<RecordBatch> {
    table
        .scan()
        .select_all()
        .build()
        .expect("scan builds")
        .to_arrow()
        .await
        .expect("scan opens")
        .try_collect()
        .await
        .expect("scan reads")
}

/// Create `name` in the IceGate namespace with `schema`.
async fn create_table_with_schema(catalog: &Arc<dyn Catalog>, name: &str, schema: Schema) -> Table {
    catalog
        .create_table(
            &NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()),
            TableCreation::builder().name(name.to_string()).schema(schema).build(),
        )
        .await
        .unwrap_or_else(|error| panic!("create {name}: {error}"))
}

/// `migrate create` brings every existing table to the code's schema in place: a table that
/// lacks trailing optional columns of the code gains them without being re-created, keeps every
/// row it held, and reads the new columns of those rows as NULL; the snapshot that stamps the
/// evolved schema keeps the committed WAL offset of a table that has one, so a reader resolving it
/// from the current snapshot still finds it. A table already on the code's schema is left alone,
/// and a second run changes nothing.
#[tokio::test]
async fn create_appends_the_trailing_optional_columns_of_the_code_and_leaves_other_tables_alone() {
    const COMMITTED_OFFSET: u64 = 42;
    let (_store, config) = setup_containers().await;
    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("namespace created");

    let mut cases = Vec::new();
    // The rows each evolved table held before the migration, with its columns at that time.
    let mut rows_before = Vec::new();
    for (name, code_schema) in code_schemas() {
        let (previous, added) = schema_before_trailing_optional_columns(&code_schema);
        let table = create_table_with_schema(&catalog, name, previous).await;
        let rows = (!added.is_empty()).then(|| {
            let arrow_schema =
                Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).expect("previous arrow schema"));
            if name == PRICES_TABLE {
                let other = RateObservation {
                    model: "other-model".to_string(),
                    ..price_row()
                };
                build_price_batch(&[price_row(), other])
                    .expect("prices batch builds")
                    .project(&(0..arrow_schema.fields().len()).collect::<Vec<_>>())
                    .expect("previous columns are a prefix")
            } else {
                two_distinct_rows(&arrow_schema)
            }
        });
        if let Some(batch) = &rows {
            rows_before.push((name, batch.clone()));
        }
        commit_fixture_snapshot(
            &catalog,
            &table,
            rows,
            (name != PRICES_TABLE).then_some(COMMITTED_OFFSET),
        )
        .await;
        let table = catalog.load_table(&icegate_table_ident(name)).await.expect("load table");
        cases.push((
            name,
            added,
            table.metadata().uuid(),
            table.metadata().snapshots().count(),
            code_schema,
        ));
    }
    // `prices` is the one table whose snapshots carry no WAL offset, so the offset check below
    // covers an evolved table only when another table evolves too.
    assert!(
        cases.iter().any(|(name, added, ..)| *name != PRICES_TABLE && !added.is_empty()),
        "the fixture must evolve a table that carries the WAL offset: {:?}",
        cases.iter().map(|(name, added, ..)| (name, added.len())).collect::<Vec<_>>()
    );

    let ops = create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("create evolves the existing tables");

    for (name, expected) in &rows_before {
        let table = catalog.load_table(&icegate_table_ident(name)).await.expect("load table");
        let rows_after = scan_rows(&table).await;
        let kept_columns: Vec<String> = expected.schema().fields().iter().map(|field| field.name().clone()).collect();
        let converter = RowConverter::new(
            expected
                .schema()
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )
        .expect("row converter builds");
        assert_eq!(
            canonical_rows(&converter, &rows_after, &kept_columns),
            canonical_rows(&converter, std::slice::from_ref(expected), &kept_columns),
            "{name}: every row survives the migration unchanged"
        );
        let (_, added, ..) = cases.iter().find(|(case, ..)| case == name).expect("case of the table");
        for batch in &rows_after {
            for column in added {
                let values = batch.column_by_name(column).expect("added column is read");
                assert_eq!(values.null_count(), batch.num_rows(), "{name}.{column} reads as NULL");
            }
        }
    }

    for (name, added, uuid, snapshot_count, code_schema) in &cases {
        let is_upgraded = ops
            .iter()
            .any(|op| matches!(op, MigrationOperation::Upgrade { table_name } if table_name == name));
        assert_eq!(is_upgraded, !added.is_empty(), "{name}: upgrade reported");
        let table = catalog.load_table(&icegate_table_ident(name)).await.expect("load table");
        let metadata = table.metadata();
        assert_eq!(metadata.uuid(), *uuid, "{name} must be evolved, not re-created");
        for column in added {
            assert!(
                metadata
                    .current_schema()
                    .field_by_name(column)
                    .is_some_and(|field| !field.required),
                "{name} must gain the optional column {column}"
            );
        }
        // The WAL and the Shifter tag Arrow data with the schema module's field ids, nested ones
        // included, so the evolved table must carry exactly those ids.
        assert_eq!(
            schema_to_arrow_schema(metadata.current_schema()).expect("evolved arrow schema"),
            schema_to_arrow_schema(code_schema).expect("code arrow schema"),
            "{name}: the evolved table must carry the schema module's field ids"
        );
        let expected_snapshots = snapshot_count + usize::from(!added.is_empty());
        assert_eq!(metadata.snapshots().count(), expected_snapshots, "{name}: snapshots");
        let current = metadata.current_snapshot().expect("current snapshot");
        assert_eq!(current.schema_id(), Some(metadata.current_schema_id()), "{name}");
        if *name == PRICES_TABLE {
            assert_eq!(
                current.summary().additional_properties.get(WAL_OFFSET_PROPERTY),
                None,
                "{name}: the pricing crawler's table must not gain a WAL offset"
            );
            assert!(
                resolve_committed_offset(metadata).is_err(),
                "{name}: a snapshot chain without an offset must not resolve"
            );
        } else {
            assert_eq!(
                current.summary().additional_properties.get(WAL_OFFSET_PROPERTY),
                Some(&COMMITTED_OFFSET.to_string()),
                "{name}: the current snapshot must carry the committed WAL offset"
            );
            assert_eq!(
                resolve_committed_offset(metadata).expect("offset resolves"),
                Some(COMMITTED_OFFSET),
                "{name}"
            );
        }
    }
    assert!(
        !ops.iter().any(|op| matches!(op, MigrationOperation::Create { .. })),
        "every table already exists: {ops:?}"
    );

    let again = create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("second create runs");
    assert!(again.is_empty(), "nothing left to evolve: {again:?}");
    for (name, added, _, snapshot_count, _) in &cases {
        let table = catalog.load_table(&icegate_table_ident(name)).await.expect("load table");
        let expected_snapshots = snapshot_count + usize::from(!added.is_empty());
        assert_eq!(
            table.metadata().snapshots().count(),
            expected_snapshots,
            "{name}: a second run adds no snapshot"
        );
    }
}

/// Catalog on which a Shifter commit lands right after the first load of `table`, so the caller
/// holds a table that is already stale when it starts its own transaction.
#[derive(Debug)]
struct CatalogWithConcurrentShiftCommit {
    inner: Arc<dyn Catalog>,
    table: TableIdent,
    shift_offset: u64,
    is_shift_committed: AtomicBool,
}

#[async_trait]
impl Catalog for CatalogWithConcurrentShiftCommit {
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(&self, namespace: &NamespaceIdent, creation: TableCreation) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        let loaded = self.inner.load_table(table).await?;
        if *table == self.table && !self.is_shift_committed.swap(true, Ordering::SeqCst) {
            commit_fixture_snapshot(&self.inner, &loaded, None, Some(self.shift_offset)).await;
        }
        Ok(loaded)
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }
}

/// A Shifter commit landing while the migration holds a stale table must not lose its offset: the
/// stamp inherits from the base the commit lands on. A stamp carrying the older offset would make
/// the Shifter resume behind what is already committed and re-commit that part of the WAL.
#[tokio::test]
async fn create_stamps_the_offset_of_a_shift_commit_that_lands_during_the_migration() {
    const OFFSET_BEFORE_MIGRATION: u64 = 42;
    const OFFSET_OF_CONCURRENT_SHIFT: u64 = 43;
    let (_store, config) = setup_containers().await;
    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("namespace created");
    let (previous, added) = schema_before_trailing_optional_columns(&operations_schema().expect("operations schema"));
    assert!(!added.is_empty(), "the fixture must evolve the table");
    let table = create_table_with_schema(&catalog, OPERATIONS_TABLE, previous).await;
    commit_fixture_snapshot(&catalog, &table, None, Some(OFFSET_BEFORE_MIGRATION)).await;
    let ident = icegate_table_ident(OPERATIONS_TABLE);
    let before = catalog.load_table(&ident).await.expect("load table");
    let racing_catalog: Arc<dyn Catalog> = Arc::new(CatalogWithConcurrentShiftCommit {
        inner: Arc::clone(&catalog),
        table: ident.clone(),
        shift_offset: OFFSET_OF_CONCURRENT_SHIFT,
        is_shift_committed: AtomicBool::new(false),
    });

    create_tables(&racing_catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("create evolves the table");

    let after = catalog.load_table(&ident).await.expect("load table");
    let metadata = after.metadata();
    assert_eq!(
        metadata.uuid(),
        before.metadata().uuid(),
        "the table must not be re-created"
    );
    for column in &added {
        assert!(metadata.current_schema().field_by_name(column).is_some(), "{column}");
    }
    assert_eq!(
        metadata.snapshots().count(),
        before.metadata().snapshots().count() + 2,
        "the racing shift commit and the stamp"
    );
    let current = metadata.current_snapshot().expect("current snapshot");
    assert_eq!(current.schema_id(), Some(metadata.current_schema_id()));
    assert_eq!(
        current.summary().additional_properties.get(WAL_OFFSET_PROPERTY),
        Some(&OFFSET_OF_CONCURRENT_SHIFT.to_string())
    );
    assert_eq!(
        resolve_committed_offset(metadata).expect("offset resolves"),
        Some(OFFSET_OF_CONCURRENT_SHIFT)
    );
}

/// A table the Shifter writes, migrated before its first shift, gains its columns and stays
/// snapshot-less: a stamp would be its only snapshot, without a WAL offset, and resolving the
/// committed offset would then refuse the table instead of starting the WAL from its beginning.
#[tokio::test]
async fn create_adds_columns_to_a_shifter_table_with_no_snapshot_without_a_snapshot() {
    let (_store, config) = setup_containers().await;
    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("namespace created");

    let mut cases = Vec::new();
    for (name, code_schema) in code_schemas() {
        if name == PRICES_TABLE {
            continue;
        }
        let (previous, added) = schema_before_trailing_optional_columns(&code_schema);
        create_table_with_schema(&catalog, name, previous).await;
        cases.push((name, added));
    }
    assert!(
        cases.iter().any(|(_, added)| !added.is_empty()),
        "the fixture must evolve a table that carries the WAL offset"
    );

    create_tables(&catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("create evolves the existing tables");

    for (name, added) in &cases {
        let table = catalog.load_table(&icegate_table_ident(name)).await.expect("load table");
        let metadata = table.metadata();
        for column in added {
            assert!(
                metadata.current_schema().field_by_name(column).is_some(),
                "{name} must gain the column {column}"
            );
        }
        assert!(
            metadata.current_snapshot().is_none(),
            "{name}: no snapshot is committed"
        );
        assert_eq!(
            resolve_committed_offset(metadata).expect("the offset of a table with no snapshot resolves"),
            None,
            "{name}: the Shifter starts the WAL from its beginning"
        );
    }
}

/// A column whose field ids in the schema module are not the ids the table assigns it is refused
/// before anything is committed. Here the table's last column id is past the schema module's
/// because a column was added and dropped before, so the table would give the appended columns
/// other ids than the WAL tags its data with. The refusal waits for the whole pass: every other
/// table is still created.
#[tokio::test]
async fn create_refuses_a_column_whose_field_ids_the_table_would_assign_differently() {
    let (_store, config) = setup_containers().await;
    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("namespace created");
    let (previous, added) = schema_before_trailing_optional_columns(&operations_schema().expect("operations schema"));
    let first_added = added.first().expect("operations ends with optional columns").clone();
    let table = create_table_with_schema(&catalog, OPERATIONS_TABLE, previous).await;
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "retired_column",
            Type::Primitive(PrimitiveType::String),
        ))
        .apply(tx)
        .expect("addition applies");
    let table = tx.commit(catalog.as_ref()).await.expect("addition commits");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .delete_column("retired_column")
        .apply(tx)
        .expect("deletion applies");
    let before = tx.commit(catalog.as_ref()).await.expect("deletion commits");

    let result = create_tables(&catalog, &SnapshotExpirationConfig::default(), false).await;

    assert!(
        matches!(&result, Err(MaintainError::Migration(message)) if message.contains(&first_added)),
        "got {result:?}"
    );
    let after = catalog
        .load_table(&icegate_table_ident(OPERATIONS_TABLE))
        .await
        .expect("load table");
    assert_eq!(
        after.metadata_location(),
        before.metadata_location(),
        "operations must not change"
    );
    for name in [LOGS_TABLE, SPANS_TABLE, EVENTS_TABLE, METRICS_TABLE, PRICES_TABLE] {
        assert!(
            catalog.table_exists(&icegate_table_ident(name)).await.expect("table_exists"),
            "{name} must be created despite the refused table"
        );
    }
}

/// `schema` with its optional top-level column `column` retyped to `Long`.
fn schema_with_retyped_column(schema: &Schema, column: &str) -> Schema {
    let fields = schema.as_struct().fields().iter().map(|field| {
        if field.name == column {
            assert!(!field.required, "the fixture retypes an optional column");
            Arc::new(NestedField::optional(
                field.id,
                field.name.clone(),
                Type::Primitive(PrimitiveType::Long),
            ))
        } else {
            Arc::clone(field)
        }
    });
    Schema::builder().with_fields(fields).build().expect("retyped schema builds")
}

/// A drift other than trailing optional columns cannot be applied in place, so `migrate create`
/// refuses it with a migration error and leaves those tables as they were. The refusal waits for
/// the whole pass: the tables after the first incompatible one are still created.
#[tokio::test]
async fn create_refuses_drift_that_is_not_a_trailing_optional_column() {
    let (_store, config) = setup_containers().await;
    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("namespace created");
    let logs = create_table_with_schema(
        &catalog,
        LOGS_TABLE,
        schema_with_retyped_column(&logs_schema().expect("logs schema"), COL_BODY),
    )
    .await;
    let spans = create_table_with_schema(
        &catalog,
        SPANS_TABLE,
        schema_with_retyped_column(&spans_schema().expect("spans schema"), COL_KIND),
    )
    .await;

    let result = create_tables(&catalog, &SnapshotExpirationConfig::default(), false).await;

    assert!(matches!(result, Err(MaintainError::Migration(_))), "got {result:?}");
    for (name, before) in [(LOGS_TABLE, &logs), (SPANS_TABLE, &spans)] {
        let after = catalog.load_table(&icegate_table_ident(name)).await.expect("load table");
        assert_eq!(
            after.metadata_location(),
            before.metadata_location(),
            "{name} must not change"
        );
    }
    for name in [EVENTS_TABLE, METRICS_TABLE, OPERATIONS_TABLE, PRICES_TABLE] {
        assert!(
            catalog.table_exists(&icegate_table_ident(name)).await.expect("table_exists"),
            "{name} must be created despite the incompatible tables"
        );
    }
}

/// A memory catalog: these tests decide what a pass over the tables does, and the commits it makes
/// reach real storage in the tests above. The namespace is left to the caller.
async fn memory_catalog() -> Arc<dyn Catalog> {
    let config = CatalogConfig {
        backend: CatalogBackend::Memory,
        warehouse: format!("memory://migrate-{}", Uuid::new_v4()),
        properties: HashMap::new(),
        cache: None,
    };
    CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("memory catalog")
}

/// [`memory_catalog`] with the IceGate namespace and an `operations` table lacking its trailing
/// optional columns. Returns the table and the names of those columns.
async fn catalog_with_previous_operations() -> (Arc<dyn Catalog>, Table, Vec<String>) {
    let catalog = memory_catalog().await;
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("namespace created");
    let (previous, added) = schema_before_trailing_optional_columns(&operations_schema().expect("operations schema"));
    assert!(!added.is_empty(), "the fixture must leave columns to append");
    let operations = create_table_with_schema(&catalog, OPERATIONS_TABLE, previous).await;
    (catalog, operations, added)
}

/// Create `logs` with a column of another type, a drift appending cannot resolve.
async fn create_retyped_logs(catalog: &Arc<dyn Catalog>) -> Table {
    create_table_with_schema(
        catalog,
        LOGS_TABLE,
        schema_with_retyped_column(&logs_schema().expect("logs schema"), COL_BODY),
    )
    .await
}

/// Assert `before` is still the current version of its table.
async fn assert_table_unchanged(catalog: &Arc<dyn Catalog>, before: &Table) {
    let after = catalog.load_table(before.identifier()).await.expect("load table");
    assert_eq!(
        after.metadata_location(),
        before.metadata_location(),
        "{} must not change",
        before.identifier().name()
    );
}

fn is_upgrade_of(op: &MigrationOperation, table: &str) -> bool {
    matches!(op, MigrationOperation::Upgrade { table_name } if table_name == table)
}

/// A dry run reports the columns `migrate create` would append and commits nothing, and it leaves
/// the columns absent, so an operator's preview cannot be what migrates the table.
#[tokio::test]
async fn dry_run_create_reports_the_columns_it_would_append_and_leaves_the_table_unchanged() {
    let (catalog, operations, added) = catalog_with_previous_operations().await;

    let ops = create_tables(&catalog, &SnapshotExpirationConfig::default(), true)
        .await
        .expect("dry run");

    assert!(ops.iter().any(|op| is_upgrade_of(op, OPERATIONS_TABLE)), "got {ops:?}");
    assert_table_unchanged(&catalog, &operations).await;
    let after = catalog.load_table(operations.identifier()).await.expect("load table");
    for column in &added {
        assert!(
            after.metadata().current_schema().field_by_name(column).is_none(),
            "{column}"
        );
    }
}

/// A dry run of `migrate create` refuses the drift the real run would refuse, touching no table.
#[tokio::test]
async fn dry_run_create_refuses_unresolvable_drift_and_leaves_the_tables_unchanged() {
    let (catalog, operations, _) = catalog_with_previous_operations().await;
    let logs = create_retyped_logs(&catalog).await;

    let result = create_tables(&catalog, &SnapshotExpirationConfig::default(), true).await;

    assert!(matches!(result, Err(MaintainError::Migration(_))), "got {result:?}");
    assert_table_unchanged(&catalog, &operations).await;
    assert_table_unchanged(&catalog, &logs).await;
}

/// Unlike `migrate create`, a dry run of `migrate upgrade` previews every mismatched table instead
/// of failing, and it tells a table it would upgrade from one a real run would refuse.
#[tokio::test]
async fn dry_run_upgrade_tells_tables_it_would_upgrade_from_tables_requiring_manual_migration() {
    let (catalog, operations, _) = catalog_with_previous_operations().await;
    let logs = create_retyped_logs(&catalog).await;

    let ops = upgrade_schemas(&catalog, true).await.expect("dry run");

    assert_eq!(ops.len(), 2, "got {ops:?}");
    assert!(ops.iter().any(|op| is_upgrade_of(op, OPERATIONS_TABLE)), "got {ops:?}");
    assert!(
        ops.iter().any(|op| matches!(
            op,
            MigrationOperation::ManualMigrationRequired { table_name, reason }
                if table_name == LOGS_TABLE && !reason.is_empty()
        )),
        "got {ops:?}"
    );
    assert_table_unchanged(&catalog, &operations).await;
    assert_table_unchanged(&catalog, &logs).await;
}

#[tokio::test]
async fn upgrade_appends_the_trailing_optional_columns_in_place() {
    let (catalog, operations, added) = catalog_with_previous_operations().await;

    let ops = upgrade_schemas(&catalog, false).await.expect("upgrade");

    assert_eq!(ops.len(), 1, "got {ops:?}");
    assert!(is_upgrade_of(&ops[0], OPERATIONS_TABLE), "got {ops:?}");
    let after = catalog.load_table(operations.identifier()).await.expect("load table");
    assert_eq!(
        after.metadata().uuid(),
        operations.metadata().uuid(),
        "the table must not be re-created"
    );
    for column in &added {
        assert!(
            after.metadata().current_schema().field_by_name(column).is_some(),
            "{column}"
        );
    }
}

/// `migrate upgrade` still evolves what it can before refusing the rest, and unlike `migrate create`
/// it never creates a missing table.
#[tokio::test]
async fn upgrade_evolves_what_it_can_refuses_the_rest_and_creates_nothing() {
    let (catalog, operations, added) = catalog_with_previous_operations().await;
    let logs = create_retyped_logs(&catalog).await;

    let result = upgrade_schemas(&catalog, false).await;

    assert!(matches!(result, Err(MaintainError::Migration(_))), "got {result:?}");
    let after = catalog.load_table(operations.identifier()).await.expect("load table");
    for column in &added {
        assert!(
            after.metadata().current_schema().field_by_name(column).is_some(),
            "{column}"
        );
    }
    assert_table_unchanged(&catalog, &logs).await;
    assert!(
        !catalog
            .table_exists(&icegate_table_ident(SPANS_TABLE))
            .await
            .expect("table_exists"),
        "a missing table is left to `migrate create`"
    );
}

/// A batch carrying columns the table lacks is refused, never projected onto the table: dropping
/// them would lose rate data without anyone noticing.
#[tokio::test]
async fn append_prices_refuses_a_batch_with_a_column_the_table_lacks() {
    let catalog = memory_catalog().await;
    let (table, _) = create_previous_prices_table(&catalog).await;

    let result = append_prices(&catalog, &[price_row()]).await;

    assert!(matches!(result, Err(MaintainError::Storage(_))), "got {result:?}");
    let after = catalog.load_table(table.identifier()).await.expect("load prices");
    assert!(after.metadata().current_snapshot().is_none(), "nothing is committed");
}

/// The pricing writer brings its batch to the table's schema: once a newer release has appended
/// an optional column to `prices`, a crawl with this binary still appends, and its rows read the
/// column this binary does not know as NULL.
#[tokio::test]
async fn append_prices_fills_a_column_the_code_lacks_with_null() {
    const EXTRA_COLUMN: &str = "added_by_a_newer_release";
    let (_store, config) = setup_containers().await;
    let catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("Failed to create catalog");
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("namespace created");
    let code_schema = prices_schema().expect("prices schema");
    let next_id = code_schema.highest_field_id() + 1;
    let wider = Schema::builder()
        .with_fields(
            code_schema
                .as_struct()
                .fields()
                .iter()
                .cloned()
                .chain([Arc::new(NestedField::optional(
                    next_id,
                    EXTRA_COLUMN,
                    Type::Primitive(PrimitiveType::String),
                ))]),
        )
        .build()
        .expect("wider prices schema builds");
    create_table_with_schema(&catalog, PRICES_TABLE, wider).await;

    let row = price_row();
    append_prices(&catalog, std::slice::from_ref(&row))
        .await
        .expect("the append into the wider table succeeds");

    let table = catalog
        .load_table(&icegate_table_ident(PRICES_TABLE))
        .await
        .expect("load prices");
    let batches = scan_rows(&table).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    for batch in &batches {
        let extra = batch.column_by_name(EXTRA_COLUMN).expect("the extra column is read");
        assert_eq!(extra.null_count(), batch.num_rows(), "the extra column is NULL");
    }
    let live = read_live_rates(&catalog).await.expect("prices read");
    assert!(live.get(&row.key()).is_some(), "the appended row is live");
}
