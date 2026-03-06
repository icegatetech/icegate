// Forked from: https://github.com/apache/iceberg-rust/blob/main/crates/integrations/datafusion/src/physical_plan/scan.rs
// Reason: upstream `IcebergTableScan` has `pub(crate)` constructor and returns `None` from `metrics()`.

//! Reimplemented Iceberg table scan with metrics and tracing.
//!
//! Forked from upstream `IcebergTableScan` (which has `pub(crate)` constructor)
//! with the following additions:
//! - `ExecutionPlanMetricsSet` with `BaselineMetrics`
//! - `metrics()` override that returns actual metrics (upstream returns `None`)
//! - Metrics tracking via `ExecutionPlanMetricsSet`

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::table::Table;
use tracing::instrument;

use super::expr_to_predicate::convert_filters_to_predicate;

/// Convert an Iceberg error into a DataFusion error.
fn to_datafusion_error(error: iceberg::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(error.into())
}

/// Iceberg table scan with metrics tracking and clean schema output.
///
/// Reimplements the upstream `IcebergTableScan` to add:
/// - Per-partition `BaselineMetrics` (`output_rows`, `output_bytes`,
///   `elapsed_compute`)
/// - Metrics tracking via `ExecutionPlanMetricsSet`
#[derive(Debug)]
pub(super) struct IcegateIcebergScan {
    /// Iceberg table instance.
    table: Table,
    /// Snapshot to scan (None = current).
    snapshot_id: Option<i64>,
    /// Pre-computed plan properties for DataFusion optimizer.
    plan_properties: PlanProperties,
    /// Projection column names, None means all columns.
    projection: Option<Vec<String>>,
    /// Iceberg predicates pushed down to the scan.
    predicates: Option<Predicate>,
    /// DataFusion metrics tracking.
    metrics: ExecutionPlanMetricsSet,
}

impl IcegateIcebergScan {
    /// Creates a new Iceberg scan node.
    ///
    /// # Arguments
    ///
    /// * `table` - The Iceberg table to scan
    /// * `snapshot_id` - Optional snapshot ID (None = current snapshot)
    /// * `schema` - The Arrow schema for this table
    /// * `projection` - Optional column indices to project
    /// * `filters` - DataFusion filter expressions to push down
    ///
    /// # Errors
    ///
    /// Returns `DataFusionError` if the projection indices are out of bounds.
    pub(super) fn try_new(
        table: Table,
        snapshot_id: Option<i64>,
        schema: &ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
    ) -> DFResult<Self> {
        let (output_schema, projection_names) = match projection {
            None => (schema.clone(), None),
            Some(proj) => {
                let projected = Arc::new(schema.project(proj)?);
                let names = projected.fields().iter().map(|f| f.name().clone()).collect::<Vec<String>>();
                (projected, Some(names))
            }
        };
        let plan_properties = Self::compute_properties(output_schema);
        let predicates = convert_filters_to_predicate(filters);

        Ok(Self {
            table,
            snapshot_id,
            plan_properties,
            projection: projection_names,
            predicates,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Compute plan properties for the DataFusion optimizer.
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for IcegateIcebergScan {
    fn name(&self) -> &'static str {
        "IcegateIcebergScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let baseline = BaselineMetrics::new(&self.metrics, partition);

        let fut = get_batch_stream(
            self.table.clone(),
            self.snapshot_id,
            self.projection.clone(),
            self.predicates.clone(),
            baseline,
        );
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), stream)))
    }
}

impl DisplayAs for IcegateIcebergScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "IcegateIcebergScan projection:[{}] predicate:[{}]",
            self.projection.as_deref().map_or(String::new(), |v| v.join(",")),
            self.predicates.as_ref().map_or_else(String::new, |p| format!("{p}"))
        )
    }
}

/// Build and execute an Iceberg table scan, tracking metrics.
///
/// Single-scan approach:
/// 1. Build one scan with projection + predicates
/// 2. `plan_files()` — collect file tasks
/// 3. Feed collected tasks to `ArrowReaderBuilder.build().read()` for data
/// 4. Track output metrics per batch
///
/// Note: `compressed_bytes` is not tracked because `FileScanTask.length` is the
/// full Parquet file size, not the compressed bytes actually read. With column
/// projection and row-group filtering, the actual I/O is much smaller than the
/// file size. The iceberg-rust `ArrowReaderBuilder` does not expose I/O-level
/// byte counters.
#[instrument(skip(table), fields(table = %table.identifier()))]
async fn get_batch_stream(
    table: Table,
    snapshot_id: Option<i64>,
    column_names: Option<Vec<String>>,
    predicates: Option<Predicate>,
    baseline: BaselineMetrics,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    // Build a single scan with projection and predicates.
    let scan_builder = snapshot_id.map_or_else(|| table.scan(), |id| table.scan().snapshot_id(id));
    let mut scan_builder = match column_names {
        Some(names) => scan_builder.select(names),
        None => scan_builder.select_all(),
    };
    if let Some(pred) = predicates {
        scan_builder = scan_builder.with_filter(pred);
    }
    let table_scan = scan_builder.build().map_err(to_datafusion_error)?;

    // Plan files: collect tasks for the ArrowReader.
    let mut file_tasks = Vec::new();
    let mut file_stream = table_scan.plan_files().await.map_err(to_datafusion_error)?;
    while let Some(task_result) = file_stream.next().await {
        let task = task_result.map_err(to_datafusion_error)?;
        file_tasks.push(Ok(task));
    }
    tracing::debug!(file_count = file_tasks.len(), "Iceberg file plan complete");

    // Feed collected tasks to ArrowReader for data reading.
    let task_stream = Box::pin(futures::stream::iter(file_tasks));
    let stream = ArrowReaderBuilder::new(table.file_io().clone())
        .with_batch_size(8192)
        .build()
        .read(task_stream)
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error);

    let mapped = stream.map(move |result| result.map(|batch| batch.record_output(&baseline)));

    Ok(Box::pin(mapped))
}
