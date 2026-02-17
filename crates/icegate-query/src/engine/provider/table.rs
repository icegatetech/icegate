//! Merged Iceberg + WAL table provider.
//!
//! `IcegateTableProvider` implements `TableProvider` and produces a
//! `UnionExec` plan that reads from both Iceberg (committed data) and WAL
//! (hot segments not yet committed). The offset boundary is determined by
//! walking the Iceberg snapshot history for the `icegate.queue.offset`
//! property.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use icegate_common::WAL_OFFSET_PROPERTY;
use object_store::ObjectStore;
use tokio_util::sync::CancellationToken;

use super::scan::IcegateIcebergScan;

/// Table provider that merges Iceberg (cold) and WAL (hot) data.
///
/// On every `scan()`, loads fresh Iceberg metadata, determines the WAL
/// boundary offset from snapshot history, and builds a `UnionExec` plan
/// that reads both sources with matching schemas.
pub(super) struct IcegateTableProvider {
    /// Iceberg catalog for loading fresh table metadata on each scan.
    catalog: Arc<dyn Catalog>,
    /// Table identifier in the catalog (namespace + name).
    table_ident: TableIdent,
    /// Arrow schema for the table.
    schema: ArrowSchemaRef,
    /// Object store URL registered with the DataFusion runtime for WAL access.
    wal_store_url: ObjectStoreUrl,
    /// Object store for WAL segment listing and HEAD requests.
    wal_store: Arc<dyn ObjectStore>,
    /// WAL base path prefix (e.g., `s3://queue/`).
    wal_base_path: String,
    /// Batch size (rows) for WAL segment reading.
    batch_size: usize,
}

impl std::fmt::Debug for IcegateTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcegateTableProvider")
            .field("table_ident", &self.table_ident)
            .field("wal_base_path", &self.wal_base_path)
            .finish_non_exhaustive()
    }
}

impl IcegateTableProvider {
    /// Creates a new merged table provider.
    ///
    /// Loads the table once to capture the Arrow schema and stores references
    /// for per-scan metadata refresh.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be loaded from the catalog or if
    /// the schema cannot be converted.
    #[tracing::instrument(skip(catalog, wal_store), fields(%table_ident))]
    pub(super) async fn try_new(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        wal_store_url: ObjectStoreUrl,
        wal_store: Arc<dyn ObjectStore>,
        wal_base_path: String,
        batch_size: usize,
    ) -> Result<Self, iceberg::Error> {
        // Load table once to capture schema
        let table = catalog.load_table(&table_ident).await?;
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            table.metadata().current_schema(),
        )?);

        let schema = arrow_schema;

        Ok(Self {
            catalog,
            table_ident,
            schema,
            wal_store_url,
            wal_store,
            wal_base_path,
            batch_size,
        })
    }
}

#[async_trait]
impl TableProvider for IcegateTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, state, projection, filters),
        fields(
            table = %self.table_ident,
            filter_count = filters.len(),
        )
    )]
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // 1. Load fresh table metadata from catalog
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        // 2. Extract WAL offset from snapshot history
        let wal_offset = extract_wal_offset(&table);
        tracing::debug!(wal_offset = ?wal_offset, "Resolved WAL boundary offset");

        // 3. Build Iceberg scan plan (our reimplemented scan with metrics)
        let has_snapshot = table.metadata().current_snapshot().is_some();
        let iceberg_plan: Option<Arc<dyn ExecutionPlan>> = if has_snapshot {
            let scan = IcegateIcebergScan::new(
                table.clone(),
                None, // Current snapshot
                &self.schema,
                projection,
                filters,
            );
            tracing::debug!("Iceberg scan plan created");
            Some(Arc::new(scan))
        } else {
            tracing::debug!("No Iceberg snapshot available, skipping Iceberg scan");
            None
        };

        // 4. Build WAL scan plan
        // Start from offset + 1 (or 0 if no offset found = fresh system)
        let wal_start = wal_offset.map_or(0, |o| o.saturating_add(1));
        let wal_plan = match self
            .build_wal_plan(state, projection, filters, wal_start, self.batch_size)
            .await
        {
            Ok(Some(plan)) => {
                tracing::debug!("WAL scan plan created");
                Some(plan)
            }
            Ok(None) => {
                tracing::debug!("No WAL segments found");
                None
            }
            Err(e) => {
                // WAL failure is non-fatal: fall back to Iceberg-only
                tracing::warn!(error = %e, "WAL reading failed, falling back to Iceberg-only");
                None
            }
        };

        // 5. Combine plans
        match (iceberg_plan, wal_plan) {
            (Some(ice), Some(wal)) => {
                tracing::debug!("Union plan: Iceberg + WAL");
                UnionExec::try_new(vec![ice, wal])
            }
            (Some(ice), None) => {
                tracing::debug!("Iceberg-only plan");
                Ok(ice)
            }
            (None, Some(wal)) => {
                tracing::debug!("WAL-only plan");
                Ok(wal)
            }
            (None, None) => {
                // Return an empty scan -- no data available from either source
                tracing::debug!("Empty plan: no Iceberg snapshot and no WAL segments");
                let scan = IcegateIcebergScan::new(table, None, &self.schema, projection, filters);
                Ok(Arc::new(scan))
            }
        }
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Inexact: both sources handle filters independently
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

impl IcegateTableProvider {
    /// Build a DataFusion execution plan for WAL Parquet segments.
    ///
    /// Lists WAL segments after `start_offset`, converts logical filter
    /// expressions to physical predicates, and configures a `DataSourceExec`
    /// with Parquet row group pruning enabled.
    #[tracing::instrument(level = "debug", skip(self, state, projection, filters), fields(start_offset))]
    async fn build_wal_plan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        start_offset: u64,
        batch_size: usize,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        // List WAL segments
        let files = list_wal_files(&self.wal_store, &self.wal_base_path, start_offset, batch_size).await?;
        if files.is_empty() {
            return Ok(None);
        }
        tracing::debug!(segment_count = files.len(), "Listed WAL segments");

        // Convert logical filter expressions to physical predicates
        // for Parquet row group pruning
        let physical_predicate = if filters.is_empty() {
            None
        } else {
            let df_schema = datafusion::common::DFSchema::try_from(self.schema.as_ref().clone())
                .map_err(|e| DataFusionError::Plan(format!("Failed to create DFSchema: {e}")))?;
            let physical_exprs: Vec<_> = filters
                .iter()
                .filter_map(|f| state.create_physical_expr(f.clone(), &df_schema).ok())
                .collect();
            if physical_exprs.is_empty() {
                None
            } else {
                // Combine with AND
                physical_exprs.into_iter().reduce(|a, b| {
                    Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                        a,
                        datafusion::logical_expr::Operator::And,
                        b,
                    ))
                })
            }
        };

        // Build ParquetSource with predicate pushdown
        let mut parquet_source = ParquetSource::default();
        if let Some(pred) = physical_predicate {
            parquet_source = parquet_source.with_predicate(pred);
        }

        // Build file scan config
        let mut builder = FileScanConfigBuilder::new(
            self.wal_store_url.clone(),
            self.schema.clone(),
            Arc::new(parquet_source),
        );
        builder = builder.with_file_group(files.into());
        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()));
        }
        let config = builder.build();

        use datafusion::catalog::memory::DataSourceExec;
        Ok(Some(DataSourceExec::from_data_source(config)))
    }
}

/// Extract the WAL boundary offset from Iceberg snapshot history.
///
/// Walks the snapshot parent chain looking for the `icegate.queue.offset`
/// property. The property may be absent from the current snapshot because
/// compaction creates snapshots without it.
///
/// Returns `None` if no snapshot has the property (fresh system: read all
/// WAL segments).
#[tracing::instrument(level = "debug", skip(table))]
fn extract_wal_offset(table: &Table) -> Option<u64> {
    /// Safety cap to prevent unbounded snapshot chain walks (e.g. a cycle or
    /// an unexpectedly deep history).
    const MAX_SNAPSHOT_WALK: u32 = 1000;

    let metadata = table.metadata();
    let mut snapshot = metadata.current_snapshot()?;
    let mut walked = 0u32;
    loop {
        if let Some(offset) = snapshot
            .summary()
            .additional_properties
            .get(WAL_OFFSET_PROPERTY)
            .and_then(|v| v.parse::<u64>().ok())
        {
            tracing::debug!(
                snapshots_walked = walked,
                offset,
                "Found WAL offset in snapshot history"
            );
            return Some(offset);
        }
        walked += 1;
        if walked >= MAX_SNAPSHOT_WALK {
            tracing::warn!(
                limit = MAX_SNAPSHOT_WALK,
                "Snapshot walk limit reached without finding WAL offset"
            );
            return None;
        }
        // Walk to parent snapshot
        let parent_id = snapshot.parent_snapshot_id()?;
        snapshot = metadata.snapshot_by_id(parent_id)?;
    }
}

/// List WAL segment files after the given offset and return as
/// `PartitionedFile` entries.
///
/// Uses `ParquetQueueReader::list_segments` to find segments, then issues
/// HEAD requests to get file sizes for DataFusion's file planning.
#[tracing::instrument(level = "debug", skip(store))]
async fn list_wal_files(
    store: &Arc<dyn ObjectStore>,
    wal_base_path: &str,
    start_offset: u64,
    batch_size: usize,
) -> DFResult<Vec<datafusion::datasource::listing::PartitionedFile>> {
    use icegate_queue::ParquetQueueReader;

    let reader = ParquetQueueReader::new(wal_base_path, Arc::clone(store), batch_size)
        .map_err(|e| DataFusionError::External(e.into()))?;

    // Intentionally uncancellable: WAL segment listing is a short metadata
    // operation during query planning that must run to completion.
    let cancel = CancellationToken::new();
    let topic: String = icegate_common::LOGS_TOPIC.to_string();
    let segments = reader
        .list_segments(&topic, start_offset, &cancel)
        .await
        .map_err(|e| DataFusionError::External(e.into()))?;
    tracing::debug!(
        segments_found = segments.len(),
        start_offset,
        "WAL segment listing complete"
    );

    // Issue HEAD requests concurrently to avoid sequential round-trip latency.
    // Each request is independent, so we use buffer_unordered for maximum
    // throughput while bounding the number of in-flight requests.
    use futures::stream::{self, StreamExt, TryStreamExt};

    const MAX_CONCURRENT_HEAD_REQUESTS: usize = 32;

    let files: Vec<datafusion::datasource::listing::PartitionedFile> = stream::iter(segments.into_iter().map(|seg| {
        let relative = seg.to_relative_path();
        let path = if wal_base_path.is_empty() {
            relative
        } else {
            object_store::path::Path::from(format!("{wal_base_path}/{relative}"))
        };

        async move {
            let store = Arc::clone(store);
            let meta = store.head(&path).await.map_err(|e| DataFusionError::External(e.into()))?;
            Ok::<_, DataFusionError>(datafusion::datasource::listing::PartitionedFile::new(
                path.to_string(),
                meta.size,
            ))
        }
    }))
    .buffer_unordered(MAX_CONCURRENT_HEAD_REQUESTS)
    .try_collect()
    .await?;

    tracing::debug!(segments_resolved = files.len(), start_offset, "WAL resolving complete");
    Ok(files)
}
