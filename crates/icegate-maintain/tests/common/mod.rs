//! Shared test utilities for icegate-maintain integration tests.
//!
//! Centralizes the object-store bootstrap contract — container start, bucket
//! creation, and concrete [`S3Catalog`] construction — shared verbatim across
//! the maintain integration tests, plus the `logs` writer path and the raw
//! object-store listing the sweep tests assert against.

// Each integration-test binary pulls this module in via `mod common;` and may
// exercise only a subset of these helpers, so unused-item lints are expected.
#![allow(dead_code, clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, MapArray, StringArray, StructArray, TimestampMicrosecondArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use iceberg::io::FileIOBuilder;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use icegate_catalog_s3::{CatalogCodecKind, S3Catalog, S3CatalogConfig};
use icegate_common::catalog::IoHandle;
use icegate_common::schema::{COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, logs_schema};
use icegate_common::storage::{OperatorRegistry, S3Config, StorageBackend, StorageConfig};
use icegate_common::testing::{S3TestContainer, create_s3_bucket, create_s3_object_store};
use object_store::ObjectStore;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

/// Object-store bucket every maintain integration test provisions and targets.
pub const BUCKET_NAME: &str = "warehouse";

/// Tenant every seeded `logs` row belongs to.
pub const TENANT: &str = "tenant-a";

/// 2026-06-11T00:00:00Z in microseconds. Rows seeded with this timestamp all
/// land in the same `(tenant_id, day)` partition, so one batch yields one file.
pub const DAY_MICROS: i64 = 1_749_600_000_000_000;

/// Connection parameters for a running object store.
#[derive(Clone)]
pub struct StorageConn {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

/// Stand up object storage and capture its connection parameters.
pub async fn setup_object_store() -> (S3TestContainer, StorageConn) {
    let store = S3TestContainer::start().await.expect("start object storage");
    create_s3_bucket(store.endpoint(), BUCKET_NAME).await.expect("create bucket");
    let conn = StorageConn {
        endpoint: store.endpoint().to_string(),
        access_key: store.username().to_string(),
        secret_key: store.password().to_string(),
    };
    (store, conn)
}

/// Build a concrete [`S3Catalog`] against the object store.
pub fn build_s3_catalog(conn: &StorageConn) -> S3Catalog {
    let io = IoHandle::noop();
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert("warehouse".to_string(), format!("s3://{BUCKET_NAME}"));
    props.insert("s3.endpoint".to_string(), conn.endpoint.clone());
    props.insert("s3.path-style-access".to_string(), "true".to_string());
    props.insert("s3.access-key-id".to_string(), conn.access_key.clone());
    props.insert("s3.secret-access-key".to_string(), conn.secret_key.clone());
    props.insert("s3.region".to_string(), "us-east-1".to_string());
    let file_io = FileIOBuilder::new(io.storage_factory()).with_props(props).build();

    S3Catalog::new(
        &S3CatalogConfig {
            bucket: BUCKET_NAME.to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some(conn.endpoint.clone()),
            access_key_id: Some(conn.access_key.clone()),
            secret_access_key: Some(conn.secret_key.clone()),
            warehouse: BUCKET_NAME.to_string(),
            codec: CatalogCodecKind::Json,
            ..S3CatalogConfig::default()
        },
        file_io,
        tokio_util::sync::CancellationToken::new(),
    )
    .expect("build S3 catalog")
}

/// The `StorageConfig` a sweep uses to build a raw object store.
///
/// The container credentials are threaded straight into the config, so a sweep
/// authenticates against this test's object store without relying on ambient
/// `AWS_*` environment variables.
pub fn storage_config(conn: &StorageConn) -> StorageConfig {
    StorageConfig {
        backend: StorageBackend::S3(S3Config {
            bucket: BUCKET_NAME.to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some(conn.endpoint.clone()),
            access_key_id: Some(conn.access_key.clone()),
            secret_access_key: Some(conn.secret_key.clone()),
        }),
    }
}

/// The registry a sweep resolves its object store through, wired the way the
/// maintain binary wires it: no read cache, the container's backend.
pub async fn build_operator_registry(conn: &StorageConn) -> Arc<OperatorRegistry> {
    IoHandle::from_config(None, Some(&storage_config(conn).backend))
        .await
        .expect("io handle")
        .object_store_operator_registry()
}

/// List every object key in the bucket (sorted) via a direct object store, so
/// assertions about what physically survives a sweep never go through the code
/// that decides what to delete.
pub async fn list_all_object_keys(conn: &StorageConn) -> Vec<String> {
    let store: Arc<dyn ObjectStore> = create_s3_object_store(&conn.endpoint, BUCKET_NAME).expect("test object store");
    let mut stream = store.list(None);
    let mut keys = Vec::new();
    while let Some(meta) = stream.next().await {
        keys.push(meta.expect("list object").location.as_ref().to_string());
    }
    keys.sort();
    keys
}

/// Build one `logs` Arrow batch from `(service_name, timestamp_micros)` rows,
/// laid out in the caller's order. The `body` column is `msg-<unique>` so every
/// row is globally distinguishable.
pub fn logs_batch(rows: &[(&str, i64)], unique_offset: usize) -> RecordBatch {
    let iceberg_schema = logs_schema().unwrap();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).unwrap());
    let n = rows.len();

    let tenant = StringArray::from(vec![TENANT; n]);
    let service_name = StringArray::from(rows.iter().map(|(s, _)| Some(*s)).collect::<Vec<_>>());
    let ts: Vec<i64> = rows.iter().map(|(_, t)| *t).collect();
    let trace_vals: Vec<[u8; 16]> = (0..n).map(|i| [u8::try_from((unique_offset + i) % 256).unwrap(); 16]).collect();
    let trace_id = FixedSizeBinaryArray::try_from_iter(trace_vals.iter().map(|v| v.to_vec())).unwrap();
    let span_vals: Vec<[u8; 8]> = (0..n).map(|i| [u8::try_from((unique_offset + i) % 256).unwrap(); 8]).collect();
    let span_id = FixedSizeBinaryArray::try_from_iter(span_vals.iter().map(|v| v.to_vec())).unwrap();
    // This fixture carries no real attribute content at any OTLP level (its
    // callers — GC sweep / snapshot expiration — exercise file lifecycle, not
    // attribute queries), so all three per-level maps are empty rather than
    // the single empty `attributes` map this used before the schema split.
    let resource_attributes = empty_string_map(&arrow_schema, COL_RESOURCE_ATTRIBUTES, n);
    let scope_attributes = empty_string_map(&arrow_schema, COL_SCOPE_ATTRIBUTES, n);
    let log_attributes = empty_string_map(&arrow_schema, COL_LOG_ATTRIBUTES, n);

    RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(tenant),
            Arc::new(service_name),
            Arc::new(TimestampMicrosecondArray::from(ts.clone())),
            Arc::new(TimestampMicrosecondArray::from(ts.clone())),
            Arc::new(TimestampMicrosecondArray::from(ts)),
            Arc::new(trace_id),
            Arc::new(span_id),
            Arc::new(StringArray::from(vec![Some("INFO"); n])),
            Arc::new(StringArray::from(
                (0..n).map(|i| Some(format!("msg-{}", unique_offset + i))).collect::<Vec<_>>(),
            )),
            resource_attributes,
            scope_attributes,
            log_attributes,
        ],
    )
    .expect("record batch")
}

/// Build an empty `MAP<Utf8,Utf8>` column of length `rows` typed exactly like
/// the named attribute-map field (`resource_attributes` / `scope_attributes`
/// / `log_attributes`) of the iceberg-derived arrow schema.
fn empty_string_map(arrow_schema: &ArrowSchema, name: &str, rows: usize) -> Arc<dyn Array> {
    let attr_field = arrow_schema.field_with_name(name).unwrap();
    let DataType::Map(entry_field, ordered) = attr_field.data_type() else {
        panic!("{name} must be a Map");
    };
    let DataType::Struct(kv_fields) = entry_field.data_type() else {
        panic!("map entry must be a Struct");
    };
    let empty_key: Arc<dyn Array> = Arc::new(StringArray::from(Vec::<&str>::new()));
    let empty_value: Arc<dyn Array> = Arc::new(StringArray::from(Vec::<&str>::new()));
    let entries = StructArray::new(kv_fields.clone(), vec![empty_key, empty_value], None);
    let offsets = OffsetBuffer::new(vec![0_i32; rows + 1].into());
    Arc::new(MapArray::new(entry_field.clone(), offsets, entries, None, *ordered))
}

/// Write a batch into the table as parquet (one file per partition); a single
/// `(tenant, day)` batch yields exactly one [`DataFile`]. The file is written to
/// object storage but committed to no snapshot — the caller decides that.
pub async fn write_one_file(table: &Table, batch: RecordBatch) -> DataFile {
    let metadata = table.metadata().clone();
    let file_io = table.file_io().clone();
    let location_generator = DefaultLocationGenerator::new(&metadata).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(Uuid::now_v7().to_string(), None, DataFileFormat::Parquet);
    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), metadata.current_schema().clone());
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        1024 * 1024 * 1024,
        file_io,
        location_generator,
        file_name_generator,
    );
    let mut fanout = FanoutWriter::new(DataFileWriterBuilder::new(rolling_builder));
    let splitter = iceberg::arrow::RecordBatchPartitionSplitter::try_new_with_computed_values(
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
    )
    .unwrap();
    for (partition_key, partition_batch) in splitter.split(&batch).unwrap() {
        fanout.write(partition_key, partition_batch).await.unwrap();
    }
    let mut files = fanout.close().await.unwrap();
    assert_eq!(files.len(), 1, "single-partition batch must yield one data file");
    files.pop().unwrap()
}
