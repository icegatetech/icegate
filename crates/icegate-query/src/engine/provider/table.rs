//! Merged Iceberg + WAL table provider.
//!
//! `IcegateTableProvider` implements `TableProvider` for any WAL-backed table
//! (currently `logs` and `spans`) and produces a `UnionExec` plan that reads
//! from both Iceberg (committed data) and WAL (hot segments not yet committed).
//! The offset boundary is determined by walking the Iceberg snapshot history
//! for the `icegate.queue.offset` property; the WAL topic to scan is supplied
//! by the caller (the schema provider) so that each table's hot data is
//! correctly scoped.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use icegate_common::resolve_wal_offset;
use icegate_queue::ParquetQueueReader;
use tokio_util::sync::CancellationToken;

use super::WalQueryConfig;
use super::scan::IcegateIcebergScan;
use crate::engine::core::WAL_STORE_URL;

/// Table provider that merges Iceberg (cold) and WAL (hot) data.
///
/// On every `scan()`, reads Iceberg metadata, determines the WAL boundary
/// offset from snapshot history, and builds a `UnionExec` plan that reads
/// both sources with matching schemas. The provider is rebuilt per session
/// by `QueryEngine::create_session()`, so the table metadata reflects the
/// latest committed state.
pub(super) struct IcegateTableProvider {
    /// Table identifier in the catalog (namespace + name).
    table_ident: TableIdent,
    /// WAL topic to scope segment listing to. By convention this matches the
    /// Iceberg table name (`logs`, `spans`). Stored as `String` because the
    /// underlying queue API expects `&Topic = &String`; the allocation
    /// happens once per provider construction.
    topic: String,
    /// Arrow schema for the table.
    schema: ArrowSchemaRef,
    /// Shared WAL queue reader for segment listing.
    wal_reader: Arc<ParquetQueueReader>,
    /// Iceberg table loaded at provider construction time.
    table: Table,
    /// WAL query configuration (enabled flag, metadata size hint).
    wal_config: WalQueryConfig,
}

impl std::fmt::Debug for IcegateTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcegateTableProvider")
            .field("table_ident", &self.table_ident)
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl IcegateTableProvider {
    /// Creates a new merged table provider.
    ///
    /// Loads the table from the catalog to capture the Arrow schema. The
    /// provider is rebuilt per session, so the table reflects the latest
    /// committed Iceberg state.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be loaded from the catalog or if
    /// the schema cannot be converted.
    #[tracing::instrument(skip(catalog, wal_reader), fields(%table_ident, %topic))]
    pub(super) async fn try_new(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        topic: &str,
        wal_reader: Arc<ParquetQueueReader>,
        wal_config: WalQueryConfig,
    ) -> Result<Self, iceberg::Error> {
        let table = catalog.load_table(&table_ident).await?;
        let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            table.metadata().current_schema(),
        )?);

        Ok(Self {
            table_ident,
            topic: topic.to_string(),
            schema,
            wal_reader,
            table,
            wal_config,
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
        // 1. Clone table (cheap: inner data is Arc-wrapped)
        let table = self.table.clone();

        // 2. Early exit when WAL is disabled — skip snapshot history walk
        let wal_enabled = self.wal_config.enabled;
        let wal_offset = if wal_enabled {
            let offset = extract_wal_offset(&table)?;
            tracing::debug!(wal_offset = ?offset, "Resolved WAL boundary offset");
            offset
        } else {
            tracing::debug!("WAL query disabled, skipping WAL offset extraction");
            None
        };

        // 3. Build Iceberg scan plan (our reimplemented scan with metrics)
        let has_snapshot = table.metadata().current_snapshot().is_some();
        let iceberg_plan: Option<Arc<dyn ExecutionPlan>> = if has_snapshot {
            let scan = IcegateIcebergScan::try_new(
                table.clone(),
                None, // Current snapshot
                &self.schema,
                projection,
                filters,
            )?;
            tracing::debug!("Iceberg scan plan created");
            Some(Arc::new(scan))
        } else {
            tracing::debug!("No Iceberg snapshot available, skipping Iceberg scan");
            None
        };

        // 4. Build WAL scan plan
        // Start from offset + 1 (or 0 if no offset found = fresh system)
        let wal_plan = if wal_enabled {
            let wal_start = wal_offset.map_or(0, |o| o.saturating_add(1));
            self.build_wal_plan(state, projection, filters, wal_start).await?
        } else {
            None
        };
        if wal_plan.is_some() {
            tracing::debug!("WAL scan plan created");
        } else {
            tracing::debug!("No WAL segments found");
        }

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
                let scan = IcegateIcebergScan::try_new(table, None, &self.schema, projection, filters)?;
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
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if !self.wal_config.enabled {
            tracing::debug!("WAL query disabled, skipping WAL scan");
            return Ok(None);
        }

        // List WAL segments via the shared reader
        let files = self.list_wal_files(start_offset).await?;
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
            let mut physical_exprs = Vec::with_capacity(filters.len());
            for f in filters {
                match state.create_physical_expr(f.clone(), &df_schema) {
                    Ok(expr) => physical_exprs.push(expr),
                    Err(e) => {
                        return Err(DataFusionError::Plan(format!(
                            "Failed to convert filter to physical expression: {e}"
                        )));
                    }
                }
            }
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

        // Build ParquetSource with predicate pushdown and metadata size hint
        let mut parquet_source = ParquetSource::new(self.schema.clone());
        if let Some(pred) = physical_predicate {
            parquet_source = parquet_source.with_predicate(pred);
        }
        if let Some(hint) = self.wal_config.metadata_size_hint {
            parquet_source = parquet_source.with_metadata_size_hint(hint);
        }
        // TODO(low): Add reverse order to optimize ORDER BY time desc

        // Build file scan config
        let wal_url = ObjectStoreUrl::parse(WAL_STORE_URL)
            .map_err(|e| DataFusionError::Plan(format!("Invalid WAL store URL: {e}")))?;
        let mut builder = FileScanConfigBuilder::new(wal_url, Arc::new(parquet_source));
        builder = builder.with_file_group(files.into());
        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()))?;
        }
        let config = builder.build();

        Ok(Some(DataSourceExec::from_data_source(config)))
    }

    /// List WAL segment files after the given offset and return as
    /// `PartitionedFile` entries.
    #[tracing::instrument(level = "debug", skip(self), fields(start_offset))]
    async fn list_wal_files(
        &self,
        start_offset: u64,
    ) -> DFResult<Vec<datafusion::datasource::listing::PartitionedFile>> {
        // Intentionally uncancellable: WAL segment listing is a short metadata
        // operation during query planning that must run to completion.
        let uncancellable_token = CancellationToken::new();
        let segment_files = self
            .wal_reader
            .list_segment_files(&self.topic, start_offset, &uncancellable_token)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        tracing::debug!(
            segments_found = segment_files.len(),
            start_offset,
            "WAL segment listing complete"
        );

        let files: Vec<datafusion::datasource::listing::PartitionedFile> = segment_files
            .into_iter()
            .map(|sf| datafusion::datasource::listing::PartitionedFile::new(sf.path, sf.size))
            .collect();

        tracing::debug!(segments_resolved = files.len(), start_offset, "WAL resolving complete");
        Ok(files)
    }
}

/// Extract the WAL boundary offset from Iceberg snapshot history.
///
/// Delegates to [`resolve_wal_offset`], which walks the snapshot parent chain
/// looking for the `icegate.queue.offset` property. The property may be absent
/// from the current snapshot because compaction creates `replace` snapshots
/// without it, so the walk skips past them to the Shifter commit that recorded
/// it. The Shifter resolves the offset through the SAME helper, keeping the
/// WAL/Iceberg boundary identical on both the read and write sides.
///
/// # Returns
///
/// * `Ok(Some(offset))` — found the WAL boundary offset
/// * `Ok(None)` — the table has NO current snapshot (a freshly created table
///   never shifted to): read all WAL segments from offset 0. This is the ONLY
///   `Ok(None)` case — a table that has snapshots but no recorded offset does
///   NOT fall through to `None`; it errors via the gate below.
///
/// # Errors
///
/// Returns `DataFusionError` when the table HAS snapshots but the offset cannot
/// be resolved — found nowhere in the reachable chain, a malformed value, a
/// missing parent snapshot, a cyclic chain, or the walk depth cap. Reading all
/// WAL segments in those cases (offset=0) would duplicate every committed row in
/// query results, so the scan fails loudly instead.
#[tracing::instrument(level = "debug", skip(table))]
fn extract_wal_offset(table: &Table) -> DFResult<Option<u64>> {
    resolve_wal_offset(table.metadata()).map_err(|e| DataFusionError::Execution(e.to_string()))
}
