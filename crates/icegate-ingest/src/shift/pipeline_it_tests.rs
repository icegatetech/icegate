//! End-to-end tests of the shift job as production assembles it: [`Shifter`] over a real WAL on
//! object storage, a real S3-backed Iceberg catalog, and S3-backed job state.
//!
//! What only this layer covers is the assembly itself - the configuration validation, the
//! `IcebergStorage` construction with the production encodings, the metrics decorators, and the
//! `plan -> shift -> commit` topology - plus the persistence the pool depends on: job state that
//! travels through object storage rather than through the in-memory backend the component tests in
//! [`super::pipeline_tests`] use. Removing a task registration from [`Shifter`] fails here, because
//! nothing in this file rebuilds that wiring.
//!
//! Requires Docker (a prerequisite):
//!
//! ```text
//! cargo test -p icegate-ingest shift::pipeline_it_tests
//! ```

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, ListArray, RecordBatch, StringArray, StructArray};
use futures::{StreamExt, TryStreamExt};
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    arrow::schema_to_arrow_schema,
    spec::{PartitionSpec, PrimitiveType, Schema, SortOrder, Type},
    table::Table,
    transaction::{AddColumn, ApplyTransactionAction, Transaction},
};
use icegate_common::{
    ICEGATE_NAMESPACE, LOGS_TABLE, LOGS_TOPIC, METRICS_TABLE, METRICS_TOPIC, OPERATIONS_TABLE, OPERATIONS_TOPIC,
    SPANS_TABLE, SPANS_TOPIC,
    catalog::{CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle},
    list_data_files_with_stats,
    parquet_encoding::{
        LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS, METRICS_BLOOM_COLUMNS, METRICS_COLUMN_ENCODINGS,
        OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS, SPANS_BLOOM_COLUMNS, SPANS_COLUMN_ENCODINGS,
    },
    parquet_writer::ColumnEncoding,
    resolve_committed_offset,
    schema::{
        logs_partition_spec, logs_schema, logs_sort_order, metrics_partition_spec, metrics_schema, metrics_sort_order,
        operations_partition_spec, operations_schema, operations_sort_order, spans_partition_spec, spans_schema,
        spans_sort_order,
    },
    testing::{S3TestContainer, create_s3_bucket, create_s3_object_store},
};
use icegate_queue::{ParquetQueueReader, PreparedWalRowGroup, QueueConfig, QueueWriter, WriteRequest, channel};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point},
    trace::v1::{
        ResourceSpans, ScopeSpans, Span,
        span::{Event, Link},
    },
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    CURRENT_PLANNER_PARTITION_SPEC, IcebergStorage, ShiftConfig, ShiftJobSpec, Shifter,
    config::JobsStorageConfig,
    iceberg_storage::WriteTarget,
    test_utils::{expected_row_bodies, log_sort_key_cmp, read_parquet_output_rows, two_tenant_ingest_batches},
};
use crate::{
    infra::metrics::ShiftMetrics,
    transform::{
        logs_to_record_batch, metrics_to_record_batch, operations_to_record_batch, spans_to_record_batch,
        test_support::test_tenant,
    },
    wal::{SortColumnsDescriptor, sort_logs, sort_metrics, sort_operations, sort_spans},
};

/// Bucket the test provisions for the WAL, the catalog, the table data, and the job state.
const BUCKET_NAME: &str = "warehouse";

/// Highest WAL offset the two-segment fixture covers: the queue numbers segments from zero.
const FIXTURE_LAST_OFFSET: u64 = 1;

/// Bound the wait for the iteration to land, so a stuck pool fails the test instead of hanging it.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(90);

/// Interval between snapshot polls while waiting for the commit task to publish its snapshot.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// The shifted `logs` rows of one parquet file, in the physical order the file stores them.
struct CommittedFile {
    tenant_id: String,
    row_bodies: Vec<String>,
}

/// Object storage, plus the credentials every component in this test authenticates with.
pub(super) struct ObjectStorage {
    _container: S3TestContainer,
    endpoint: String,
    access_key: String,
    secret_key: String,
}

/// Start object storage and create the bucket the run works in.
pub(super) async fn start_object_storage() -> ObjectStorage {
    let container = S3TestContainer::start().await.expect("start object storage");
    create_s3_bucket(container.endpoint(), BUCKET_NAME)
        .await
        .expect("create bucket");
    ObjectStorage {
        endpoint: container.endpoint().to_string(),
        access_key: container.username().to_string(),
        secret_key: container.password().to_string(),
        _container: container,
    }
}

/// Write `segments` into the WAL through the production writer, one segment each, and answer
/// the queue reader the shifter consumes them with.
///
/// The writer runs with the production bloom-filter and encoding lists the caller passes for
/// `topic`, so the segments a shift task reads back are encoded the way the ingest binary encodes
/// them.
pub(super) async fn write_wal_segments(
    storage: &ObjectStorage,
    base_path: &str,
    topic: &str,
    encodings: (&'static [&'static str], &'static [ColumnEncoding]),
    segments: Vec<Vec<PreparedWalRowGroup>>,
) -> Arc<ParquetQueueReader> {
    let (bloom_filter_columns, column_encodings) = encodings;
    let store = create_s3_object_store(&storage.endpoint, BUCKET_NAME).expect("wal object store");
    let queue_config = QueueConfig::new(base_path);
    let max_row_group_size = queue_config.common.max_row_group_size;
    let (write_tx, write_rx) = channel(queue_config.common.channel_capacity);
    let writer = QueueWriter::new(queue_config, Arc::clone(&store))
        .with_bloom_filter_columns(HashMap::from([(topic.to_string(), bloom_filter_columns)]))
        .with_column_encodings(HashMap::from([(topic.to_string(), column_encodings)]));
    let writer_handle = writer.start(write_rx).await.expect("start WAL writer");

    for (segment_idx, row_groups) in segments.into_iter().enumerate() {
        let (response_tx, response_rx) = oneshot::channel();
        write_tx
            .send(WriteRequest {
                topic: topic.to_string(),
                row_groups,
                response_tx,
                trace_context: None,
            })
            .await
            .expect("enqueue WAL write");
        let result = response_rx.await.expect("WAL write result");
        assert_eq!(
            result.offset(),
            Some(segment_idx as u64),
            "the fixture depends on the segment offsets the queue assigns"
        );
    }

    drop(write_tx);
    writer_handle.await.expect("WAL writer task").expect("WAL writer shutdown");

    Arc::new(ParquetQueueReader::new(base_path.to_string(), store, max_row_group_size).expect("queue reader"))
}

/// Build the S3-backed catalog the ingest binary builds, with the IceGate namespace created.
pub(super) async fn build_catalog(storage: &ObjectStorage, catalog_prefix: &str) -> Arc<dyn Catalog> {
    let catalog_config = CatalogConfig {
        backend: CatalogBackend::S3 {
            warehouse: catalog_prefix.to_string(),
        },
        warehouse: BUCKET_NAME.to_string(),
        properties: HashMap::from([
            ("bucket".to_string(), BUCKET_NAME.to_string()),
            ("region".to_string(), "us-east-1".to_string()),
            ("endpoint".to_string(), storage.endpoint.clone()),
            ("access_key_id".to_string(), storage.access_key.clone()),
            ("secret_access_key".to_string(), storage.secret_key.clone()),
        ]),
        cache: None,
    };
    let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("S3 catalog");
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("create namespace");
    catalog
}

/// Create `table` in the IceGate namespace with the schema, partition spec, and sort order the
/// schema module defines for it.
pub(super) async fn create_table(
    catalog: &Arc<dyn Catalog>,
    table: &str,
    schema: Schema,
    partition_spec: PartitionSpec,
    sort_order: SortOrder,
) -> TableIdent {
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(table.to_string())
                .schema(schema)
                .partition_spec(partition_spec.into_unbound())
                .sort_order(sort_order)
                .build(),
        )
        .await
        .unwrap_or_else(|error| panic!("create {table} table: {error}"));
    TableIdent::new(namespace, table.to_string())
}

/// Build the S3-backed catalog and create the `logs` table in it.
async fn create_logs_table(storage: &ObjectStorage, catalog_prefix: &str) -> (Arc<dyn Catalog>, TableIdent) {
    let catalog = build_catalog(storage, catalog_prefix).await;
    let schema = logs_schema().expect("logs schema");
    let partition_spec = logs_partition_spec(&schema).expect("logs partition spec");
    let sort_order = logs_sort_order(&schema).expect("logs sort order");
    let ident = create_table(&catalog, LOGS_TABLE, schema, partition_spec, sort_order).await;
    (catalog, ident)
}

/// The shift configuration the run uses: production defaults, with the job state pointed at this
/// run's own prefix in the bucket and a poll interval short enough to keep the test brief.
pub(super) fn shift_config(storage: &ObjectStorage, jobs_prefix: &str) -> ShiftConfig {
    let mut config = ShiftConfig::default();
    config.jobsmanager.worker_count = 4;
    config.jobsmanager.poll_interval_ms = 50;
    config.jobsmanager.storage = JobsStorageConfig {
        endpoint: Some(storage.endpoint.clone()),
        bucket: BUCKET_NAME.to_string(),
        prefix: jobs_prefix.to_string(),
        access_key_id: Some(storage.access_key.clone()),
        secret_access_key: Some(storage.secret_key.clone()),
        ..JobsStorageConfig::default()
    };
    config
}

/// Poll the table until a snapshot carries a committed WAL offset, and answer it.
///
/// The pool publishes the snapshot from a worker, so the test cannot observe the commit directly;
/// what it can observe is the offset the snapshot claims, which no earlier step writes.
async fn wait_for_committed_offset(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> u64 {
    let deadline = Instant::now() + COMMIT_TIMEOUT;
    loop {
        let table = catalog.load_table(ident).await.expect("load table");
        if let Some(offset) = resolve_committed_offset(table.metadata()).expect("read committed WAL offset") {
            return offset;
        }
        assert!(
            Instant::now() < deadline,
            "the shift iteration did not commit a snapshot within {COMMIT_TIMEOUT:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Read every parquet file the current snapshot references, decoded into its tenant and its rows
/// in physical order.
async fn read_committed_files(table: &Table) -> Vec<CommittedFile> {
    let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");
    let data_files = list_data_files_with_stats(table, descriptor)
        .await
        .expect("current snapshot data files");

    let mut committed = Vec::with_capacity(data_files.len());
    for data_file in data_files {
        let path = data_file.data_file.file_path().to_string();
        let rows = read_parquet_output_rows(table.file_io(), &path).await;
        let tenant_id = rows.first().expect("parquet rows").tenant_id.clone();
        assert!(
            rows.iter().all(|row| row.tenant_id == tenant_id),
            "one parquet file must hold one tenant partition, got {path}"
        );
        for window in rows.windows(2) {
            assert_ne!(
                log_sort_key_cmp(&window[0], &window[1]),
                std::cmp::Ordering::Greater,
                "parquet rows of {path} must be monotonic by the logs sort order"
            );
        }
        committed.push(CommittedFile {
            tenant_id,
            row_bodies: rows.into_iter().filter_map(|row| row.body).collect(),
        });
    }
    committed.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
    committed
}

/// One iteration of the production shifter over a two-segment WAL: every tenant of the fan-out
/// reaches Iceberg exactly once, in sort-key order, under a snapshot claiming the WAL offset the
/// plan saw.
///
/// Multi-threaded, with as many runtime threads as [`shift_config`] gives the pool workers: a shift
/// task merges row groups and encodes parquet synchronously, so on a single-threaded runtime it
/// would hold the thread and stall the other workers and the timers this test waits on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_production_shifter_commits_every_tenant_of_the_wal_in_sort_key_order() {
    let storage = start_object_storage().await;
    let run_id = Uuid::new_v4().simple().to_string();
    // Two row groups per segment, so a shift task has something to merge across row groups
    // rather than copying a single sorted block through.
    let segments = two_tenant_ingest_batches()
        .iter()
        .map(|batch| {
            sort_logs(batch, 2, None)
                .expect("prepare WAL segment")
                .expect("segment row groups")
                .write_request
                .row_groups
        })
        .collect();
    let queue_reader = write_wal_segments(
        &storage,
        &format!("wal-{run_id}"),
        LOGS_TOPIC,
        (LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS),
        segments,
    )
    .await;
    let (catalog, ident) = create_logs_table(&storage, &format!("catalog-{run_id}")).await;

    let config = shift_config(&storage, &format!("jobs-{run_id}"));
    let jobs_storage = config.jobsmanager.storage.to_s3_config().expect("job state storage");
    let jobs = &[ShiftJobSpec {
        job_name: "shift_logs",
        topic: LOGS_TOPIC,
        table: LOGS_TABLE,
        descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
        planner_partition_spec: &CURRENT_PLANNER_PARTITION_SPEC,
        bloom_filter_columns: LOGS_BLOOM_COLUMNS,
        column_encodings: LOGS_COLUMN_ENCODINGS,
    }];
    let shifter = Shifter::new_with_max_iterations(
        Arc::clone(&catalog),
        queue_reader,
        Arc::new(config),
        jobs_storage,
        ShiftMetrics::new_disabled(),
        Arc::new(jobmanager::NoopMetrics),
        jobs,
        Some(1),
    )
    .await
    .expect("build the production shifter");

    let handle = shifter.start().expect("start the shifter");
    let committed_offset = wait_for_committed_offset(&catalog, &ident).await;
    handle.shutdown().await.expect("shut the shifter down");

    assert_eq!(
        committed_offset, FIXTURE_LAST_OFFSET,
        "the snapshot must claim the highest WAL offset the plan task saw"
    );

    let table = catalog.load_table(&ident).await.expect("load committed logs table");
    let committed = read_committed_files(&table).await;
    assert_eq!(
        committed.iter().map(|file| file.tenant_id.as_str()).collect::<Vec<_>>(),
        vec!["tenant-a", "tenant-b"],
        "the fan-out must commit one file per tenant"
    );
    for file in &committed {
        assert_eq!(
            file.row_bodies,
            expected_row_bodies(&file.tenant_id),
            "tenant '{}' must be stored in sort-key order with WAL-stable tie-breakers",
            file.tenant_id
        );
    }
}

/// String-valued OTLP attribute.
fn kv_str(key: &str, value: &str) -> KeyValue {
    kv(key, Value::StringValue(value.to_string()))
}

/// Double-valued OTLP attribute.
fn kv_dbl(key: &str, value: f64) -> KeyValue {
    kv(key, Value::DoubleValue(value))
}

/// Integer-valued OTLP attribute.
fn kv_int(key: &str, value: i64) -> KeyValue {
    kv(key, Value::IntValue(value))
}

/// OTLP attribute carrying `value`.
fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key_strindex: 0,
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
    }
}

/// Trace every span of [`evaluated_traces_request`] belongs to.
const EVALUATED_TRACE_ID: [u8; 16] = [7u8; 16];
/// The evaluated LLM span.
const EVALUATED_SPAN_ID: [u8; 8] = [1u8; 8];
/// The post-hoc evaluator span linked to [`EVALUATED_SPAN_ID`].
const CARRIER_SPAN_ID: [u8; 8] = [2u8; 8];
/// An LLM span nothing evaluates.
const PLAIN_SPAN_ID: [u8; 8] = [3u8; 8];

/// A span of [`EVALUATED_TRACE_ID`] with the given id and attributes.
fn traced_span(span_id: [u8; 8], attributes: Vec<KeyValue>) -> Span {
    Span {
        trace_id: EVALUATED_TRACE_ID.to_vec(),
        span_id: span_id.to_vec(),
        name: "op".to_string(),
        start_time_unix_nano: 1_700_000_000_000_000_000,
        end_time_unix_nano: 1_700_000_001_000_000_000,
        attributes,
        ..Span::default()
    }
}

/// One trace in which an LLM span is evaluated inline — through an `OpenInference` attribute
/// array and an OTEL `GenAI` result event — and again post hoc by an evaluator span linked to
/// it, beside an LLM span nothing evaluates.
pub(super) fn evaluated_traces_request() -> ExportTraceServiceRequest {
    let mut evaluated = traced_span(
        EVALUATED_SPAN_ID,
        vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("evaluations.0.evaluation.name", "hallucination"),
            kv_int("evaluations.0.evaluation.score", 1),
            kv_str("evaluations.0.evaluation.annotator_kind", "LLM"),
        ],
    );
    evaluated.events = vec![Event {
        time_unix_nano: 1_700_000_000_500_000_000,
        name: "gen_ai.evaluation.result".to_string(),
        attributes: vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_dbl("gen_ai.evaluation.score.value", 0.9),
            kv_str("gen_ai.evaluation.score.label", "relevant"),
        ],
        dropped_attributes_count: 0,
    }];
    let mut carrier = traced_span(
        CARRIER_SPAN_ID,
        vec![
            kv_str("openinference.span.kind", "EVALUATOR"),
            kv_str("evaluations.0.evaluation.name", "groundedness"),
            kv_str("evaluations.0.evaluation.label", "grounded"),
        ],
    );
    carrier.links = vec![Link {
        trace_id: EVALUATED_TRACE_ID.to_vec(),
        span_id: EVALUATED_SPAN_ID.to_vec(),
        ..Link::default()
    }];
    let plain = traced_span(PLAIN_SPAN_ID, vec![kv_str("gen_ai.operation.name", "chat")]);
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![evaluated, carrier, plain],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
}

/// One evaluation result as a query engine reads it back from the table.
#[derive(Debug, PartialEq)]
pub(super) struct StoredEvaluation {
    name: String,
    score_value: Option<f64>,
    score_label: Option<String>,
    annotator_kind: Option<String>,
    target_scope: String,
    target_trace_id: Option<Vec<u8>>,
    target_span_id: Option<Vec<u8>>,
}

/// Decode every row of `batch` into its span id and its `evaluations` list, each field
/// addressed by name.
pub(super) fn decode_evaluations(batch: &RecordBatch) -> Vec<(Vec<u8>, Option<Vec<StoredEvaluation>>)> {
    let span_ids = batch
        .column_by_name("span_id")
        .expect("span_id column")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("span_id is FixedSizeBinary");
    let evaluations = batch
        .column_by_name("evaluations")
        .expect("evaluations column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("evaluations is List");
    (0..batch.num_rows())
        .map(|row| {
            let results = (!evaluations.is_null(row)).then(|| {
                let elements = evaluations.value(row);
                let elements = elements.as_any().downcast_ref::<StructArray>().expect("elements are Struct");
                let text = |field: &str, index: usize| {
                    let column = elements
                        .column_by_name(field)
                        .unwrap_or_else(|| panic!("element field {field}"))
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap_or_else(|| panic!("{field} is Utf8"));
                    (!column.is_null(index)).then(|| column.value(index).to_string())
                };
                let id = |field: &str, index: usize| {
                    let column = elements
                        .column_by_name(field)
                        .unwrap_or_else(|| panic!("element field {field}"))
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap_or_else(|| panic!("{field} is FixedSizeBinary"));
                    (!column.is_null(index)).then(|| column.value(index).to_vec())
                };
                let scores = elements
                    .column_by_name("score_value")
                    .expect("element field score_value")
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("score_value is Float64");
                (0..elements.len())
                    .map(|index| StoredEvaluation {
                        name: text("name", index).expect("a stored result is always named"),
                        score_value: (!scores.is_null(index)).then(|| scores.value(index)),
                        score_label: text("score_label", index),
                        annotator_kind: text("annotator_kind", index),
                        target_scope: text("target_scope", index).expect("a stored result always has a scope"),
                        target_trace_id: id("target_trace_id", index),
                        target_span_id: id("target_span_id", index),
                    })
                    .collect()
            });
            (span_ids.value(row).to_vec(), results)
        })
        .collect()
}

/// The rows [`decode_evaluations`] reads back from the table after [`evaluated_traces_request`] is
/// shifted into it, in span id order.
pub(super) fn expected_stored_evaluations() -> Vec<(Vec<u8>, Option<Vec<StoredEvaluation>>)> {
    let evaluated_trace = Some(EVALUATED_TRACE_ID.to_vec());
    let evaluated_span = Some(EVALUATED_SPAN_ID.to_vec());
    vec![
        (
            EVALUATED_SPAN_ID.to_vec(),
            Some(vec![
                StoredEvaluation {
                    name: "hallucination".to_string(),
                    score_value: Some(1.0),
                    score_label: None,
                    annotator_kind: Some("LLM".to_string()),
                    target_scope: "span".to_string(),
                    target_trace_id: evaluated_trace.clone(),
                    target_span_id: evaluated_span.clone(),
                },
                StoredEvaluation {
                    name: "Relevance".to_string(),
                    score_value: Some(0.9),
                    score_label: Some("relevant".to_string()),
                    annotator_kind: None,
                    target_scope: "span".to_string(),
                    target_trace_id: evaluated_trace.clone(),
                    target_span_id: evaluated_span.clone(),
                },
            ]),
        ),
        (
            CARRIER_SPAN_ID.to_vec(),
            Some(vec![StoredEvaluation {
                name: "groundedness".to_string(),
                score_value: None,
                score_label: Some("grounded".to_string()),
                annotator_kind: None,
                target_scope: "span".to_string(),
                target_trace_id: evaluated_trace,
                target_span_id: evaluated_span,
            }]),
        ),
        (PLAIN_SPAN_ID.to_vec(), None),
    ]
}

/// Evaluation results survive the whole ingest path: the OTLP transform, the WAL the production
/// writer encodes, the production shifter, and an Iceberg commit. They are read back through an
/// Iceberg table scan, which binds the nested `evaluations` fields by field id, as a query engine
/// reading the table does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evaluation_results_reach_iceberg_through_the_production_shifter() {
    let storage = start_object_storage().await;
    let run_id = Uuid::new_v4().simple().to_string();

    let (batch, drops) = operations_to_record_batch(&evaluated_traces_request(), &test_tenant("tenant-a"))
        .expect("operations transform");
    assert_eq!(drops, 0);
    let prepared = sort_operations(&batch.expect("three operations rows"), 2, None)
        .expect("prepare WAL segment")
        .expect("segment row groups");
    let queue_reader = write_wal_segments(
        &storage,
        &format!("wal-{run_id}"),
        OPERATIONS_TOPIC,
        (OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS),
        vec![prepared.write_request.row_groups],
    )
    .await;

    let catalog = build_catalog(&storage, &format!("catalog-{run_id}")).await;
    let schema = operations_schema().expect("operations schema");
    let partition_spec = operations_partition_spec(&schema).expect("operations partition spec");
    let sort_order = operations_sort_order(&schema).expect("operations sort order");
    let ident = create_table(&catalog, OPERATIONS_TABLE, schema, partition_spec, sort_order).await;

    let config = shift_config(&storage, &format!("jobs-{run_id}"));
    let jobs_storage = config.jobsmanager.storage.to_s3_config().expect("job state storage");
    let jobs = &[ShiftJobSpec {
        job_name: "shift_operations",
        topic: OPERATIONS_TOPIC,
        table: OPERATIONS_TABLE,
        descriptor: SortColumnsDescriptor::operations().expect("operations descriptor"),
        planner_partition_spec: &CURRENT_PLANNER_PARTITION_SPEC,
        bloom_filter_columns: OPERATIONS_BLOOM_COLUMNS,
        column_encodings: OPERATIONS_COLUMN_ENCODINGS,
    }];
    let shifter = Shifter::new_with_max_iterations(
        Arc::clone(&catalog),
        queue_reader,
        Arc::new(config),
        jobs_storage,
        ShiftMetrics::new_disabled(),
        Arc::new(jobmanager::NoopMetrics),
        jobs,
        Some(1),
    )
    .await
    .expect("build the production shifter");

    let handle = shifter.start().expect("start the shifter");
    let committed_offset = wait_for_committed_offset(&catalog, &ident).await;
    handle.shutdown().await.expect("shut the shifter down");
    assert_eq!(committed_offset, 0, "the snapshot must claim the one WAL segment");

    let table = catalog.load_table(&ident).await.expect("load committed operations table");
    let mut stream = table
        .scan()
        .build()
        .expect("operations scan")
        .to_arrow()
        .await
        .expect("operations scan stream");
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.expect("operations batch") {
        rows.extend(decode_evaluations(&batch));
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0));

    assert_eq!(
        rows,
        expected_stored_evaluations(),
        "every row must keep exactly the results it was ingested with"
    );
}

/// A minimal OTLP logs request: one record, enough for the logs transform to emit a row.
fn single_record_logs_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("hello".to_string())),
                    }),
                    ..LogRecord::default()
                }],
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    }
}

/// A minimal OTLP metrics request: one gauge data point, enough for the metrics transform to
/// emit a row.
fn single_gauge_metrics_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "cpu".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_700_000_000_000_000_000,
                            value: Some(number_data_point::Value::AsDouble(1.0)),
                            ..NumberDataPoint::default()
                        }],
                    })),
                    ..Metric::default()
                }],
                ..ScopeMetrics::default()
            }],
            ..ResourceMetrics::default()
        }],
    }
}

/// One WAL-fed table and the WAL segment its production ingest path writes into its topic.
struct WalTableCase {
    table: &'static str,
    topic: &'static str,
    encodings: (&'static [&'static str], &'static [ColumnEncoding]),
    schema: Schema,
    partition_spec: PartitionSpec,
    sort_order: SortOrder,
    row_groups: Vec<PreparedWalRowGroup>,
}

/// The WAL-fed tables that have a WAL topic, each with one segment built by its production
/// transform and WAL sorter.
fn wal_table_cases() -> Vec<WalTableCase> {
    let tenant = test_tenant("tenant-a");
    let logs_batch = logs_to_record_batch(&single_record_logs_request(), &tenant)
        .expect("logs transform")
        .expect("one logs row");
    let (spans_batch, _) = spans_to_record_batch(&evaluated_traces_request(), &tenant).expect("spans transform");
    let (metrics_batch, _) =
        metrics_to_record_batch(&single_gauge_metrics_request(), &tenant).expect("metrics transform");
    let (operations_batch, _) =
        operations_to_record_batch(&evaluated_traces_request(), &tenant).expect("operations transform");
    let logs = logs_schema().expect("logs schema");
    let spans = spans_schema().expect("spans schema");
    let metrics = metrics_schema().expect("metrics schema");
    let operations = operations_schema().expect("operations schema");
    vec![
        WalTableCase {
            table: LOGS_TABLE,
            topic: LOGS_TOPIC,
            encodings: (LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS),
            partition_spec: logs_partition_spec(&logs).expect("logs partition spec"),
            sort_order: logs_sort_order(&logs).expect("logs sort order"),
            schema: logs,
            row_groups: sort_logs(&logs_batch, 2, None)
                .expect("prepare logs segment")
                .expect("logs row groups")
                .write_request
                .row_groups,
        },
        WalTableCase {
            table: SPANS_TABLE,
            topic: SPANS_TOPIC,
            encodings: (SPANS_BLOOM_COLUMNS, SPANS_COLUMN_ENCODINGS),
            partition_spec: spans_partition_spec(&spans).expect("spans partition spec"),
            sort_order: spans_sort_order(&spans).expect("spans sort order"),
            schema: spans,
            row_groups: sort_spans(&spans_batch.expect("spans rows"), 2, None)
                .expect("prepare spans segment")
                .expect("spans row groups")
                .write_request
                .row_groups,
        },
        WalTableCase {
            table: METRICS_TABLE,
            topic: METRICS_TOPIC,
            encodings: (METRICS_BLOOM_COLUMNS, METRICS_COLUMN_ENCODINGS),
            partition_spec: metrics_partition_spec(&metrics).expect("metrics partition spec"),
            sort_order: metrics_sort_order(&metrics).expect("metrics sort order"),
            schema: metrics,
            row_groups: sort_metrics(&metrics_batch.expect("metrics rows"), 2, None)
                .expect("prepare metrics segment")
                .expect("metrics row groups")
                .write_request
                .row_groups,
        },
        WalTableCase {
            table: OPERATIONS_TABLE,
            topic: OPERATIONS_TOPIC,
            encodings: (OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS),
            partition_spec: operations_partition_spec(&operations).expect("operations partition spec"),
            sort_order: operations_sort_order(&operations).expect("operations sort order"),
            schema: operations,
            row_groups: sort_operations(&operations_batch.expect("operations rows"), 2, None)
                .expect("prepare operations segment")
                .expect("operations row groups")
                .write_request
                .row_groups,
        },
    ]
}

/// A batch read back from the WAL carries, by value, the Arrow schema of the table it is shifted
/// into — field-id metadata of nested fields included.
///
/// The schema transition in front of the writer passes such a batch through untouched and
/// rebuilds any other; were the two to differ, every shift would take the rebuild path and a
/// nested column would fail its strict type comparison.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wal_batch_carries_the_arrow_schema_of_its_table() {
    let storage = start_object_storage().await;
    let run_id = Uuid::new_v4().simple().to_string();
    let catalog = build_catalog(&storage, &format!("catalog-{run_id}")).await;
    let cancel = CancellationToken::new();

    for case in wal_table_cases() {
        let queue_reader = write_wal_segments(
            &storage,
            &format!("wal-{run_id}-{}", case.topic),
            case.topic,
            case.encodings,
            vec![case.row_groups],
        )
        .await;
        let ident = create_table(&catalog, case.table, case.schema, case.partition_spec, case.sort_order).await;
        let table = catalog.load_table(&ident).await.expect("load table");
        let table_schema =
            Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).expect("table arrow schema"));

        let batches = queue_reader
            .read_segment(&case.topic.to_string(), 0, &[0], &cancel)
            .await
            .expect("open WAL segment")
            .try_collect::<Vec<_>>()
            .await
            .expect("read WAL segment");
        let batch = batches.first().expect("one WAL batch");
        assert_eq!(
            batch.schema(),
            table_schema,
            "a '{}' WAL batch must carry the table's Arrow schema",
            case.table
        );
    }
}

/// A write attempt writes into the table it loaded, even when a migration adds a column and the
/// table is read again between that load and the write — what an expired cache TTL or a sibling
/// shift task's reload does while the migrate hook commits. Were the writer to load the table
/// again, the batch built for the loaded schema would lack the new column and be refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_lands_in_the_table_its_attempt_loaded_when_a_column_is_added_before_the_write() {
    let storage = start_object_storage().await;
    let run_id = Uuid::new_v4().simple().to_string();
    let (catalog, ident) = create_logs_table(&storage, &format!("catalog-{run_id}")).await;
    let shift_storage = IcebergStorage::new(
        Arc::clone(&catalog),
        LOGS_TABLE,
        &shift_config(&storage, &format!("jobs-{run_id}")),
        64 * 1024 * 1024,
        LOGS_BLOOM_COLUMNS,
        LOGS_COLUMN_ENCODINGS,
    );
    let cancel = CancellationToken::new();
    let batch = logs_to_record_batch(&single_record_logs_request(), &test_tenant("tenant-a"))
        .expect("logs transform")
        .expect("one logs row");

    let target = shift_storage.load_write_target(&cancel).await.expect("load write target");
    let table = catalog.load_table(&ident).await.expect("load logs table");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "environment",
            Type::Primitive(PrimitiveType::String),
        ))
        .apply(tx)
        .expect("add column applies");
    tx.commit(catalog.as_ref()).await.expect("add column commits");
    let reloaded = shift_storage.reload_write_target(&cancel).await.expect("reload write target");
    assert!(
        reloaded.arrow_schema().field_with_name("environment").is_ok(),
        "the fixture must evolve the table the next load reads"
    );

    let written = shift_storage
        .write_record_batches(target, futures::stream::once(async { Ok(batch) }).boxed(), &cancel)
        .await
        .expect("the write into the loaded table must not be refused");

    assert_eq!(written.rows_written, 1);
    assert_eq!(written.data_files.len(), 1);
}
