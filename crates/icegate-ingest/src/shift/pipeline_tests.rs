//! Component tests of the shift job: `plan -> shift -> commit` driven by a real worker pool.
//!
//! The pool runs on the jobmanager's in-memory backend, so no object storage is involved, and the
//! executors registered are the production ones. What is under test here is therefore the
//! orchestration the plan task performs - the fan-out of shift tasks, the dependency edge that
//! holds the commit task back, and the outputs the commit task reads off its dependencies - which
//! no test reaching a runner directly can observe.

use std::{sync::Arc, time::Duration};

use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::parquet_encoding::{LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS};
use icegate_queue::QueueReader;
use jobmanager::{JobCode, JobsManager, TaskDefinition, TaskExecutor, TaskOutcome, task_fn};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::InMemoryMetricExporter;
use uuid::Uuid;

use super::{
    COMMIT_TASK_CODE, CURRENT_PLANNER_PARTITION_SPEC, CommitExecutor, CommitInput, PLAN_TASK_CODE, PlanExecutor,
    SHIFT_TASK_CODE, ShiftConfig, ShiftExecutor,
    commit_runner::CommitTaskRunnerImpl,
    iceberg_storage::{IcebergStorage, Storage, writer_max_parquet_bytes},
    instrumentation::{
        CommitTaskRunnerWithMetrics, PlanTaskRunnerWithMetrics, QueueReaderWithMetrics,
        RowGroupsMergerObserverWithMetrics, ShiftTaskRunnerWithMetrics, StorageWithMetrics,
    },
    plan_runner::PlanTaskRunnerImpl,
    shift_runner::ShiftTaskRunnerImpl,
    test_utils::{
        IcebergStorageWithHistory, LogOutputRow, RecordingStorage, StorageCall, StorageFault, WalQueueReader,
        WalSegment, assert_parquet_row_group_bounds_match_sorted_rows, expected_row_bodies, generous_timeouts,
        log_sort_key_cmp, logs_ingest_batch_with_schema, read_parquet_output_rows, row_bodies_from_batches,
        tenant_ids_from_batches, two_tenant_ingest_batches_with_schema,
    },
};
use crate::{
    infra::metrics::{
        ShiftMetrics,
        test_support::{build_meter_provider, find_counter_total, find_gauge_value, find_histogram_count},
    },
    wal::{SortColumnsDescriptor, sort_logs},
};

/// Job code the fixture pool runs under; the production code is per signal (`shift_logs`).
const JOB_CODE: &str = "shift_logs";

/// Workers behind the fixture pool. More than one, so the fan-out of an iteration is executed
/// concurrently the way production executes it (production sizes the pool from the CPU count, see
/// `ShiftJobsManagerConfig::default`).
///
/// Every test driving [`run_shift_job`] runs on a multi-threaded runtime of the same size: a shift
/// task does its merge and its parquet encoding synchronously, so on the single-threaded default
/// the workers would only interleave at await points and the concurrency this count asks for would
/// not exist. The attribute cannot read this constant, so the two are kept equal by hand.
const WORKER_COUNT: usize = 4;

/// Upper bound on one fixture job. Every wait in a test is bounded, and this one is wide enough
/// that a loaded machine does not turn a passing run into a failure.
const JOB_COMPLETION_TIMEOUT: Duration = Duration::from_secs(60);

/// Highest WAL offset the two-segment fixture below covers.
const FIXTURE_LAST_OFFSET: u64 = 101;

/// Runs the shift job on an in-memory worker pool until it has run out `max_iterations`.
///
/// Several workers, because the fan-out under test is concurrent in production: the shift tasks of
/// one iteration are meant to run on different workers while the commit task waits for all of them.
/// The dependency edges are enforced by the job state rather than by the worker count, so no
/// assertion here may name which worker ran what.
///
/// The dependencies are decorated the way the production assembly decorates them: the queue reader
/// once for both runners that read it, the storage the shift and commit tasks share, each of the
/// three runners, and the merger observer the shift runner reports its row groups to. The plan task
/// reads storage undecorated, as production leaves it. Segment read parallelism comes from the same
/// configuration field production reads it from, so a fixture cannot drift away from the
/// configured path. The exporter every decorator records into is returned so a test can assert on
/// the labels. The production assembly itself - configuration validation, storage construction, and
/// the task topology - is covered by [`super::pipeline_it_tests`], which drives [`super::Shifter`].
///
/// Panics rather than returning an error: every failure here is a broken fixture, and a test that
/// has to unwrap the harness first reads worse than one that starts at its own assertions.
async fn run_shift_job<Q, S>(
    queue_reader: Arc<Q>,
    storage: Arc<S>,
    shift_config: Arc<ShiftConfig>,
    table: &str,
    max_iterations: u64,
) -> InMemoryMetricExporter
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
    let (meter_provider, metric_exporter) = build_meter_provider();
    let metrics = ShiftMetrics::new(&meter_provider.meter("shift_pipeline_tests"));
    let timeouts = generous_timeouts();
    let plan_timeout = timeouts.plan_timeout();
    let queue_reader = Arc::new(QueueReaderWithMetrics::new(queue_reader, metrics.clone()));
    let plan_runner = Arc::new(PlanTaskRunnerWithMetrics::new(
        Arc::new(PlanTaskRunnerImpl::new(
            Arc::clone(&queue_reader),
            Arc::clone(&storage),
            Arc::clone(&shift_config),
            timeouts,
            table,
            &CURRENT_PLANNER_PARTITION_SPEC,
        )),
        metrics.clone(),
        table,
    ));
    let shift_storage = Arc::new(StorageWithMetrics::new(storage, metrics.clone(), table));
    let shift_runner = Arc::new(ShiftTaskRunnerWithMetrics::new(
        Arc::new(
            ShiftTaskRunnerImpl::new(
                Arc::clone(&queue_reader),
                Arc::clone(&shift_storage),
                table,
                shift_config.write.row_group_size,
                SortColumnsDescriptor::logs().expect("logs descriptor"),
            )
            .expect("non-zero output_batch_size must be accepted")
            .with_row_groups_merger_observer(Arc::new(RowGroupsMergerObserverWithMetrics::new(
                metrics.clone(),
                table,
            )))
            .with_segment_read_parallelism(shift_config.read.shift_segment_read_parallelism)
            .expect("the configured segment read parallelism must be accepted"),
        ),
        metrics.clone(),
        table,
    ));
    let commit_runner = Arc::new(CommitTaskRunnerWithMetrics::new(
        Arc::new(CommitTaskRunnerImpl::new(Arc::clone(&shift_storage), table)),
        metrics,
        table,
    ));

    let manager = JobsManager::builder()
        .in_memory()
        .workers(WORKER_COUNT)
        .poll_interval(Duration::from_millis(5))
        .job(JOB_CODE, move |job| {
            job.max_iterations(max_iterations);
            job.add_task(
                TaskDefinition::new(PLAN_TASK_CODE, plan_timeout),
                Arc::new(PlanExecutor::new(plan_runner)),
            );
            job.add_task_executor(SHIFT_TASK_CODE, Arc::new(ShiftExecutor::new(shift_runner)));
            job.add_task_executor(COMMIT_TASK_CODE, Arc::new(CommitExecutor::new(commit_runner)));
        })
        .build()
        .await
        .expect("shift job pool must build");

    let handle = manager.start().expect("worker pool must start");
    let job_code = JobCode::new(JOB_CODE);
    let completion = tokio::time::timeout(JOB_COMPLETION_TIMEOUT, handle.wait_for_job_completion(&job_code)).await;
    handle.shutdown().await.expect("worker pool must shut down");
    completion
        .expect("shift job must finish inside the bounded wait")
        .expect("shift job must reach a terminal iteration");

    meter_provider.force_flush().expect("recorded metrics must be exported");
    metric_exporter
}

/// Runs a job of one task on an in-memory worker pool until its single iteration is terminal.
///
/// Used where the case is about how one runner reacts to the state the pool hands it, rather than
/// about the fan-out the plan task builds.
async fn run_single_task_job(definition: TaskDefinition, executor: Arc<dyn TaskExecutor>) {
    let job_code = JobCode::new(JOB_CODE);
    let manager = JobsManager::builder()
        .in_memory()
        .workers(1)
        .poll_interval(Duration::from_millis(5))
        .job(job_code.clone(), move |job| {
            job.max_iterations(1);
            job.add_task(definition, executor);
        })
        .build()
        .await
        .expect("single task pool must build");

    let handle = manager.start().expect("worker pool must start");
    let completion = tokio::time::timeout(JOB_COMPLETION_TIMEOUT, handle.wait_for_job_completion(&job_code)).await;
    handle.shutdown().await.expect("worker pool must shut down");
    completion
        .expect("the job must finish inside the bounded wait")
        .expect("the iteration must reach a terminal state");
}

/// Two WAL segments holding two tenants, which the planner splits into one shift task per tenant.
///
/// Built against a freshly derived schema; see [`logs_ingest_batch_with_schema`] for when that is
/// (and is not) safe. A test that writes through the real catalog must use
/// [`two_tenant_wal_segments_with_schema`] instead.
fn two_tenant_wal_segments() -> Vec<WalSegment> {
    two_tenant_wal_segments_with_schema(&crate::transform::logs_arrow_schema().expect("logs arrow schema"))
}

/// As [`two_tenant_wal_segments`], but built against an explicit Arrow schema — see
/// [`logs_ingest_batch_with_schema`].
fn two_tenant_wal_segments_with_schema(schema: &arrow::datatypes::Schema) -> Vec<WalSegment> {
    let [ingest_segment_100, ingest_segment_101] = two_tenant_ingest_batches_with_schema(schema);
    let prepared_100 = sort_logs(&ingest_segment_100, 2, None)
        .expect("prepare WAL segment 100")
        .expect("segment 100 row groups");
    let prepared_101 = sort_logs(&ingest_segment_101, 2, None)
        .expect("prepare WAL segment 101")
        .expect("segment 101 row groups");
    vec![
        WalSegment {
            offset: 100,
            row_groups: prepared_100.write_request.row_groups,
        },
        WalSegment {
            offset: FIXTURE_LAST_OFFSET,
            row_groups: prepared_101.write_request.row_groups,
        },
    ]
}

/// Parquet paths recorded for the write calls, flattened in arrival order.
fn written_parquet_files(calls: &[StorageCall]) -> Vec<String> {
    calls
        .iter()
        .filter_map(|call| match call {
            StorageCall::Write(paths) => Some(paths.clone()),
            StorageCall::Commit { .. } => None,
        })
        .flatten()
        .collect()
}

/// The whole point of the fan-out: the commit task is created with the shift tasks as its
/// dependencies, runs only once every one of them completed, and commits exactly the parquet files
/// they reported as their output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_runs_after_every_shift_task_and_commits_the_files_they_produced() {
    let wal_segments = two_tenant_wal_segments();
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));
    let storage = Arc::new(RecordingStorage::new());

    run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::new(ShiftConfig::default()),
        "logs",
        1,
    )
    .await;

    let calls = storage.calls().await;
    let mut written = written_parquet_files(&calls);
    assert_eq!(written.len(), 2, "one shift task per tenant must write one file each");

    let Some(StorageCall::Commit {
        parquet_files,
        last_offset,
    }) = calls.last().cloned()
    else {
        panic!("the last storage call must be the commit, got: {calls:?}");
    };
    assert_eq!(
        last_offset, FIXTURE_LAST_OFFSET,
        "the commit must carry the highest planned WAL offset"
    );
    let mut committed = parquet_files;
    committed.sort();
    written.sort();
    assert_eq!(
        committed, written,
        "the commit must receive exactly the files its dependencies wrote"
    );
    assert_eq!(
        calls.iter().filter(|call| matches!(call, StorageCall::Commit { .. })).count(),
        1
    );
}

/// The second iteration plans above the offset the first one committed, so the queue offers it
/// nothing and it shifts nothing. This is the mechanism that keeps an iteration from re-shifting
/// segments that are already in the table; the already-committed guard behind it is the fallback,
/// not the first line of defence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_iteration_plans_nothing_above_the_committed_offset() {
    let wal_segments = two_tenant_wal_segments();
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));
    let storage = Arc::new(RecordingStorage::new());

    run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::new(ShiftConfig::default()),
        "logs",
        2,
    )
    .await;

    let calls = storage.calls().await;
    assert_eq!(
        storage.commits().await.len(),
        1,
        "only the iteration that found segments may commit"
    );
    assert_eq!(
        written_parquet_files(&calls).len(),
        2,
        "the second iteration must not rewrite the segments the first one shifted"
    );
}

/// The idle topic, which is the state a plan task finds most of the time: no segment above the
/// committed offset, so the iteration finishes without a shift task, a parquet file, or a snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_topic_completes_the_iteration_without_writing_or_committing() {
    let queue_reader = Arc::new(WalQueueReader::new(&[]));
    let storage = Arc::new(RecordingStorage::new());

    run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::new(ShiftConfig::default()),
        "logs",
        1,
    )
    .await;

    assert!(
        storage.calls().await.is_empty(),
        "an idle topic must neither write nor commit"
    );
}

/// A shift task that fails takes the commit task's dependency with it: the commit stays blocked
/// and the iteration ends without a snapshot, rather than committing a partial fan-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_shift_task_leaves_the_commit_unexecuted() {
    let wal_segments = two_tenant_wal_segments();
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));
    let storage = Arc::new(RecordingStorage::with_fault(StorageFault::Write));

    run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::new(ShiftConfig::default()),
        "logs",
        1,
    )
    .await;

    assert!(
        storage.commits().await.is_empty(),
        "nothing may be committed when a shift task never produced its parquet files"
    );
}

/// Only one tenant's write fails, so one shift task of the fan-out completes and one does not. The
/// dependency edge is what has to hold here: a commit over the half that succeeded would claim a
/// WAL offset covering rows that were never written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partially_failed_fan_out_commits_nothing() {
    let wal_segments = two_tenant_wal_segments();
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));
    let storage = Arc::new(RecordingStorage::with_fault(StorageFault::WriteForTenant("tenant-b")));

    run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::new(ShiftConfig::default()),
        "logs",
        1,
    )
    .await;

    assert_eq!(
        storage.written_batches().await.len(),
        1,
        "the fixture must reach the case under test: one tenant written, one failed"
    );
    assert!(
        storage.commits().await.is_empty(),
        "a fan-out that lost one of its shift tasks must not be committed"
    );
    assert_eq!(storage.committed_offset().await, None);
}

/// Each step the commit task depends on can fail on its own, and none of them may leave a snapshot
/// behind: the WAL offset would then claim rows the table does not hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_that_cannot_read_its_inputs_creates_no_snapshot() {
    for fault in [
        StorageFault::CommitLastOffsetRead,
        StorageFault::DataFilesRead,
        StorageFault::Commit,
    ] {
        let wal_segments = two_tenant_wal_segments();
        let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));
        let storage = Arc::new(RecordingStorage::with_fault(fault));

        run_shift_job(
            queue_reader,
            Arc::clone(&storage),
            Arc::new(ShiftConfig::default()),
            "logs",
            1,
        )
        .await;

        assert_eq!(
            storage.committed_offset().await,
            None,
            "a commit that failed at {fault:?} must leave no snapshot"
        );
    }
}

/// A task input the pool hands back unparseable means the persisted job state is corrupted. The
/// runner must fail its own task over it - not panic, and not touch storage on the way out.
#[tokio::test]
async fn a_task_whose_input_payload_is_malformed_fails_without_touching_storage() {
    let timeouts = generous_timeouts();
    let shift_storage = Arc::new(RecordingStorage::new());
    let commit_storage = Arc::new(RecordingStorage::new());
    let shift_runner = Arc::new(
        ShiftTaskRunnerImpl::new(
            Arc::new(WalQueueReader::new(&[])),
            Arc::clone(&shift_storage),
            "logs",
            ShiftConfig::default().write.row_group_size,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted"),
    );
    let commit_runner = Arc::new(CommitTaskRunnerImpl::new(Arc::clone(&commit_storage), "logs"));
    let malformed_tasks: Vec<(&str, Arc<dyn TaskExecutor>, Arc<RecordingStorage>)> = vec![
        (
            SHIFT_TASK_CODE,
            Arc::new(ShiftExecutor::new(shift_runner)),
            Arc::clone(&shift_storage),
        ),
        (
            COMMIT_TASK_CODE,
            Arc::new(CommitExecutor::new(commit_runner)),
            Arc::clone(&commit_storage),
        ),
    ];

    for (task_code, executor, storage) in malformed_tasks {
        // One attempt each: the task is expected to fail, and retrying it only makes the test wait
        // for the same failure four more times.
        run_single_task_job(
            TaskDefinition::new(task_code, timeouts.shift_timeout(1, 1).expect("task timeout"))
                .with_input(b"not json".to_vec())
                .with_max_attempts(1),
            executor,
        )
        .await;

        assert!(
            storage.calls().await.is_empty(),
            "task '{task_code}' must not reach storage with a payload it could not parse"
        );
    }
}

/// A commit whose response was lost has already created its snapshot. The attempt that follows
/// must recognise the offset it finds and finish without a second one - otherwise every lost
/// response doubles the rows of an iteration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_whose_response_was_lost_creates_exactly_one_snapshot_on_retry() {
    let wal_segments = two_tenant_wal_segments();
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));
    let storage = Arc::new(RecordingStorage::with_fault(StorageFault::CommitResponseLostOnce));

    run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::new(ShiftConfig::default()),
        "logs",
        1,
    )
    .await;

    let calls = storage.calls().await;
    assert_eq!(
        storage.commits().await.len(),
        1,
        "the attempt after a lost response must not create a second snapshot"
    );
    assert_eq!(
        storage.committed_offset().await,
        Some(FIXTURE_LAST_OFFSET),
        "the snapshot the lost response hid must still carry the planned WAL offset"
    );
    assert_eq!(
        written_parquet_files(&calls).len(),
        2,
        "retrying the commit must not rewrite the parquet files of its dependencies"
    );
}

/// Metric labels are an operational contract - dashboards and alerts key off them - and the task
/// codes are what an on-call reads first. A clean iteration must report a success for each of the
/// three, the number of shift tasks it planned, and the WAL backlog it measured.
///
/// The same iteration also has to report what its dependencies did, which is what pins the rest of
/// the production decorator chain: the queue read the plan task issued, the parquet write of every
/// shift task, the commit, and one row-group lifetime per row group the merger opened. Those
/// measurements exist only while the queue, the storage, and the merger observer stay wrapped, so
/// an assembly that drops one of the wrappers fails here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successful_iteration_records_success_for_every_task_code() {
    let wal_segments = two_tenant_wal_segments();
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));
    let storage = Arc::new(RecordingStorage::new());

    let metric_exporter = run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::new(ShiftConfig::default()),
        "logs",
        1,
    )
    .await;

    for (task_code, expected_successes) in [(PLAN_TASK_CODE, 1), (SHIFT_TASK_CODE, 2), (COMMIT_TASK_CODE, 1)] {
        assert_eq!(
            find_counter_total(
                &metric_exporter,
                "icegate_ingest_shift_task_success",
                &[("task", task_code), ("topic", "logs")],
            ),
            expected_successes,
            "task '{task_code}' must report its successful runs"
        );
    }
    assert_eq!(
        find_counter_total(
            &metric_exporter,
            "icegate_ingest_shift_planned_tasks",
            &[("topic", "logs")],
        ),
        2,
        "the plan must report the shift tasks it created"
    );
    // The backlog spans WAL offsets, not segments: from the first uncommitted offset through the
    // last one the plan saw, both ends included.
    assert_eq!(
        find_gauge_value(
            &metric_exporter,
            "icegate_ingest_shift_backlog_segments",
            &[("topic", "logs")],
        ),
        Some(FIXTURE_LAST_OFFSET + 1),
        "the plan must report the WAL backlog it measured"
    );

    // Taken from the fixture rather than written out: the merger opens every row group the plan
    // hands to a shift task, and how many row groups the WAL sorter produced is not this test's
    // contract.
    let planned_row_groups = wal_segments.iter().map(|segment| segment.row_groups.len() as u64).sum::<u64>();
    for (metric_name, expected_measurements) in [
        ("icegate_ingest_shift_queue_plan_duration", 1),
        ("icegate_ingest_shift_parquet_write_duration", 2),
        ("icegate_ingest_shift_commit_duration", 1),
    ] {
        assert_eq!(
            find_histogram_count(&metric_exporter, metric_name, &[("status", "ok"), ("topic", "logs")]),
            expected_measurements,
            "'{metric_name}' must report the calls the iteration made"
        );
    }
    assert_eq!(
        find_histogram_count(
            &metric_exporter,
            "icegate_ingest_shift_row_groups_merger_row_group_lifetime_duration",
            &[("topic", "logs")],
        ),
        planned_row_groups,
        "the merger must report the lifetime of every row group it opened"
    );
}

/// A shift task that failed is reported under the typed reason it failed with, not under a generic
/// label, and the commit task it blocked is reported neither as a success nor as a failure - it
/// never ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_shift_task_records_its_typed_reason() {
    let wal_segments = two_tenant_wal_segments();
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));
    let storage = Arc::new(RecordingStorage::with_fault(StorageFault::Write));

    let metric_exporter = run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::new(ShiftConfig::default()),
        "logs",
        1,
    )
    .await;

    // Every attempt of a failing task reports its own failure, and how many attempts a task gets is
    // the pool's own budget, so the count is bounded from below only.
    assert!(
        find_counter_total(
            &metric_exporter,
            "icegate_ingest_shift_task_failures",
            &[("task", SHIFT_TASK_CODE), ("reason", "write"), ("topic", "logs")],
        ) >= 1,
        "a failed write must be reported under the write reason"
    );
    assert_eq!(
        find_counter_total(
            &metric_exporter,
            "icegate_ingest_shift_task_success",
            &[("task", COMMIT_TASK_CODE), ("topic", "logs")],
        ),
        0,
        "a commit that never ran must not be reported as a success"
    );
    assert_eq!(
        find_counter_total(
            &metric_exporter,
            "icegate_ingest_shift_task_failures",
            &[("task", COMMIT_TASK_CODE), ("topic", "logs")],
        ),
        0,
        "a commit that never ran must not be reported as a failure either"
    );
}

/// Guard, not a production scenario: the commit task refuses a fan-out that produced nothing. A
/// dependency finishing with an empty output means no parquet file was written, and committing
/// then would create a snapshot claiming a WAL offset whose rows are not in the table.
///
/// The dependency is a stub because the assembled pipeline never produces one: a shift task that
/// wrote no parquet file fails, and a task with no segments to shift - the one case the production
/// runner does complete with an empty output - is never created, because every planned chunk
/// carries at least one row group and a plan holding none fails the plan task instead. The other
/// two `NoParquet` branches are reachable from a test just as well and are documented as guards at
/// their site.
///
/// What the pool exposes is the commit, not the reason it was refused, so this pins the absence of
/// a snapshot rather than the `NoParquet` classification behind it.
#[tokio::test]
async fn a_commit_guard_refuses_a_dependency_that_produced_no_output() {
    let storage = Arc::new(RecordingStorage::new());
    let commit_runner = Arc::new(CommitTaskRunnerImpl::new(Arc::clone(&storage), "logs"));
    let timeouts = generous_timeouts();
    let shift_timeout = timeouts.shift_timeout(1, 1).expect("shift timeout");
    let commit_timeout = timeouts.commit_timeout(1).expect("commit timeout");
    let commit_input = serde_json::to_vec(&CommitInput {
        last_offset: FIXTURE_LAST_OFFSET,
        trace_context: None,
    })
    .expect("commit input");

    let job_code = JobCode::new(JOB_CODE);
    let manager = JobsManager::builder()
        .in_memory()
        .workers(1)
        .poll_interval(Duration::from_millis(5))
        .job(job_code.clone(), move |job| {
            job.max_iterations(1);
            // One attempt each: the commit task is expected to fail, and retrying it only makes
            // the test wait for the same failure four more times.
            let shift_task = job.add_task(
                TaskDefinition::new(SHIFT_TASK_CODE, shift_timeout).with_max_attempts(1),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            );
            let commit_task = job.add_task(
                TaskDefinition::new(COMMIT_TASK_CODE, commit_timeout)
                    .with_input(commit_input)
                    .with_max_attempts(1),
                Arc::new(CommitExecutor::new(commit_runner)),
            );
            job.depends_on(commit_task, &[shift_task]);
        })
        .build()
        .await
        .expect("commit guard pool must build");

    let handle = manager.start().expect("worker pool must start");
    let completion = tokio::time::timeout(JOB_COMPLETION_TIMEOUT, handle.wait_for_job_completion(&job_code)).await;
    handle.shutdown().await.expect("worker pool must shut down");
    completion
        .expect("the job must finish inside the bounded wait")
        .expect("the failing iteration must still reach a terminal state");

    assert!(
        storage.commits().await.is_empty(),
        "a commit task whose dependency wrote nothing must not create a snapshot"
    );
}

/// Partition isolation and merge order of what each shift task hands to the writer: a task covers
/// one tenant, and inside it the rows follow the logs sort key with WAL order breaking ties.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_shift_task_writes_one_tenant_in_sort_key_order() {
    let wal_segments = two_tenant_wal_segments();
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));
    let storage = Arc::new(RecordingStorage::new());

    run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::new(ShiftConfig::default()),
        "logs",
        1,
    )
    .await;

    let written_batches = storage.written_batches().await;
    assert_eq!(written_batches.len(), 2, "two tenants must produce two writes");
    let mut seen_tenants = Vec::new();
    for batches in &written_batches {
        let tenant_ids = tenant_ids_from_batches(batches);
        let tenant = tenant_ids.first().expect("written tenant ids").clone();
        assert!(
            tenant_ids.iter().all(|tenant_id| tenant_id == &tenant),
            "partition bucket boundary violated: expected one tenant per output, got {tenant_ids:?}"
        );
        assert_eq!(
            row_bodies_from_batches(batches),
            expected_row_bodies(&tenant),
            "merged order must match the sort key and WAL-stable tie-breakers"
        );
        seen_tenants.push(tenant);
    }
    seen_tenants.sort();
    assert_eq!(seen_tenants, vec!["tenant-a".to_string(), "tenant-b".to_string()]);
}

/// The same pipeline against real parquet and a real Iceberg commit: what lands on disk must be
/// sorted, tenant-isolated, and reachable through a snapshot carrying the committed WAL offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pipeline_commits_sorted_parquet_and_records_the_wal_offset() {
    // The table is created before the WAL fixture: `create_logs_table` uses the `Memory`
    // catalog backend, which reassigns nested field ids (map children) on `create_table` —
    // unlike the production `S3Catalog`, which preserves them verbatim (see
    // `IcebergTableMetadata::create` in `icegate-catalog-s3/src/domain/root.rs`). So the
    // fixture batches must be built against the table's own committed schema, not a freshly
    // derived one — see `two_tenant_wal_segments_with_schema`.
    let table_name = format!("logs_e2e_{}", Uuid::new_v4().simple());
    let (catalog, table) = super::test_utils::create_logs_table(&table_name).await;
    let table_arrow_schema = schema_to_arrow_schema(table.metadata().current_schema()).expect("table arrow schema");
    let wal_segments = two_tenant_wal_segments_with_schema(&table_arrow_schema);
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));

    let mut shift_config = ShiftConfig::default();
    shift_config.write.row_group_size = 2;
    let shift_config = Arc::new(shift_config);
    let storage = Arc::new(IcebergStorageWithHistory::new(IcebergStorage::new(
        Arc::clone(&catalog),
        table_name.clone(),
        shift_config.as_ref(),
        writer_max_parquet_bytes(shift_config.read.upper_bound_input_mb_per_task * 1024 * 1024),
        LOGS_BLOOM_COLUMNS,
        LOGS_COLUMN_ENCODINGS,
    )));

    run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::clone(&shift_config),
        &table_name,
        1,
    )
    .await;

    let written_files = storage.written_data_files().await;
    assert_eq!(
        written_files.len(),
        2,
        "two tenant shift tasks must write two parquet files"
    );

    let mut seen_tenants = Vec::new();
    for data_file in &written_files {
        let rows = read_parquet_output_rows(table.file_io(), data_file.file_path()).await;
        let tenant = rows.first().expect("parquet rows").tenant_id.clone();
        assert!(
            rows.iter().all(|row| row.tenant_id == tenant),
            "one parquet file must contain one tenant partition"
        );
        for window in rows.windows(2) {
            assert_ne!(
                log_sort_key_cmp(&window[0], &window[1]),
                std::cmp::Ordering::Greater,
                "parquet rows must be monotonic by logs sort order"
            );
        }
        assert_eq!(
            rows.iter()
                .filter_map(|row: &LogOutputRow| row.body.clone())
                .collect::<Vec<_>>(),
            expected_row_bodies(&tenant),
            "parquet rows must preserve WAL-stable order for equal sort keys"
        );
        assert_parquet_row_group_bounds_match_sorted_rows(table.file_io(), data_file.file_path()).await;
        seen_tenants.push(tenant);
    }
    seen_tenants.sort();
    assert_eq!(seen_tenants, vec!["tenant-a".to_string(), "tenant-b".to_string()]);

    assert_eq!(
        storage
            .get_last_offset(&tokio_util::sync::CancellationToken::new())
            .await
            .expect("committed offset"),
        Some(FIXTURE_LAST_OFFSET),
        "the snapshot the commit task created must carry the planned WAL offset"
    );

    let mut written_paths = written_files
        .iter()
        .map(|data_file| data_file.file_path().to_string())
        .collect::<Vec<_>>();
    let current_table = catalog
        .load_table(&iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(icegate_common::ICEGATE_NAMESPACE.to_string()),
            table_name,
        ))
        .await
        .expect("current logs table");
    let mut committed_paths = icegate_common::list_data_files_with_stats(
        &current_table,
        SortColumnsDescriptor::logs().expect("logs descriptor"),
    )
    .await
    .expect("current snapshot data files")
    .into_iter()
    .map(|data_file| data_file.data_file.file_path().to_string())
    .collect::<Vec<_>>();
    written_paths.sort();
    committed_paths.sort();
    assert_eq!(
        committed_paths, written_paths,
        "the current snapshot must reference every parquet file written by the commit"
    );
}

/// Behavioural pin of the writer failover budget.
///
/// `IcebergStorage` uses a writer rollover budget of
/// `upper_bound_bytes * WRITER_FILE_SIZE_FAILOVER_FACTOR`. In normal operation the planner shapes
/// one chunk per shift task and the writer never rolls over; rollover is the failover path. This
/// test forces it by giving storage a pathologically small budget while the planner runs with the
/// default config, so a single shift task covers the whole tenant and its encoded parquet output
/// (footer + statistics + data) overshoots the budget on every row group.
///
/// The paired "no rollover under the default config" invariant is pinned by
/// [`the_pipeline_commits_sorted_parquet_and_records_the_wal_offset`], which asserts one file per
/// tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pathologically_small_writer_budget_rolls_the_shift_task_over_several_data_files() {
    // The table is created before the WAL fixture — see the comment in
    // `the_pipeline_commits_sorted_parquet_and_records_the_wal_offset` for why the batch must
    // carry the table's own committed field ids rather than a freshly derived schema's.
    let table_name = format!("logs_failover_{}", Uuid::new_v4().simple());
    let (catalog, table) = super::test_utils::create_logs_table(&table_name).await;
    let table_arrow_schema = schema_to_arrow_schema(table.metadata().current_schema()).expect("table arrow schema");

    let ingest_segment = logs_ingest_batch_with_schema(
        &table_arrow_schema,
        vec![
            ("tenant-a", Some("svc-1"), 100, 101),
            ("tenant-a", Some("svc-1"), 100, 102),
            ("tenant-a", Some("svc-0"), 110, 103),
            ("tenant-a", Some("svc-2"), 120, 104),
            ("tenant-a", Some("svc-3"), 130, 105),
            ("tenant-a", Some("svc-4"), 140, 106),
        ],
    );
    let prepared = sort_logs(&ingest_segment, 2, None)
        .expect("prepare WAL segment")
        .expect("segment row groups");
    let wal_segments = vec![WalSegment {
        offset: 100,
        row_groups: prepared.write_request.row_groups,
    }];
    let queue_reader = Arc::new(WalQueueReader::new(&wal_segments));

    let mut shift_config = ShiftConfig::default();
    shift_config.write.row_group_size = 2;
    let shift_config = Arc::new(shift_config);
    let storage = Arc::new(IcebergStorageWithHistory::new(IcebergStorage::new(
        Arc::clone(&catalog),
        table_name.clone(),
        shift_config.as_ref(),
        // 2 bytes: every row group the rolling writer flushes overshoots it and starts a new file.
        writer_max_parquet_bytes(1),
        &[],
        &[],
    )));

    run_shift_job(
        queue_reader,
        Arc::clone(&storage),
        Arc::clone(&shift_config),
        &table_name,
        1,
    )
    .await;

    assert!(
        storage.written_data_files().await.len() > 1,
        "a tiny writer budget must roll the single shift task over into multiple parquet files"
    );
}
