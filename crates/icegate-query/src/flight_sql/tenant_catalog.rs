//! Tenant-scoped catalog wrappers that enforce `tenant_id = '<t>'` on
//! every scan, regardless of the SQL path used to reach the table.
//!
//! ## Why this exists
//!
//! The session provider previously hid the raw `iceberg` catalog after
//! creating tenant-filtered SQL views — a perimeter defence where a
//! single missed lookup path could leak rows across tenants. This module
//! flips the model to defence-in-depth: the `iceberg.icegate.<table>`
//! paths stay fully visible, but the `TableProvider`s returned for them
//! are wrapped so that every `scan()`:
//!
//! 1. pushes `tenant_id = '<t>'` down into the inner scan, so Iceberg
//!    partition pruning and Parquet row-group pruning still skip other
//!    tenants' files before any data is read, and
//! 2. wraps the inner plan in a [`FilterExec`] that re-applies the same
//!    predicate at the **row** level, then projects `tenant_id` back out.
//!
//! Step 2 is the load-bearing guarantee. The merged WAL + Iceberg
//! provider reports `Inexact` filter pushdown — it prunes with the
//! predicate but relies on a parent `FilterExec` for exact row-level
//! filtering. When a client types `WHERE`, DataFusion builds that
//! `FilterExec`. Our tenant predicate is injected *below* the planner, so
//! DataFusion never builds one for it; without step 2 the un-partitioned
//! WAL hot segments — whose row-groups interleave tenants and so cannot
//! be pruned by `tenant_id` statistics — would be read but not filtered,
//! leaking other tenants' rows. Building the `FilterExec` here makes
//! tenant isolation a property of *this* wrapper, independent of how any
//! inner provider chooses to honour pushed-down filters.
//!
//! ## Hiding `tenant_id` from clients
//!
//! On top of enforcing the predicate, the wrapper removes the
//! `tenant_id` column from the schema it advertises. Clients see an
//! N-1 column table with no notion of tenancy: `SELECT *`, `DESCRIBE`,
//! `information_schema.columns`, and Flight SQL
//! `get_tables(include_schema=true)` all reflect the filtered schema.
//! This removes the entire surface for "leak my own tenant id back to
//! me" foot-guns and prevents clients from accidentally sorting,
//! grouping, or joining on what is effectively an internal partition
//! key.
//!
//! The inner scan is asked for the visible columns *plus* `tenant_id`
//! (needed to evaluate the row-level filter); the trailing `tenant_id`
//! column is then dropped by a [`ProjectionExec`] so the batches handed
//! back match the advertised schema exactly.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::{DFSchema, Statistics};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType, col, lit};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use icegate_common::schema::COL_TENANT_ID;

/// `CatalogProvider` decorator that returns tenant-scoped schemas.
///
/// Constructed once per Flight SQL request from the engine's shared
/// catalog provider; cheap to clone (`Arc` only) so it can be handed
/// to `SessionContext::register_catalog` without further care.
#[derive(Debug)]
pub(crate) struct TenantScopedCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    tenant_id: Arc<str>,
}

impl TenantScopedCatalogProvider {
    /// Wrap a catalog provider so all its tables expose tenant-scoped
    /// scans.
    pub(crate) fn new(inner: Arc<dyn CatalogProvider>, tenant_id: impl Into<Arc<str>>) -> Self {
        Self {
            inner,
            tenant_id: tenant_id.into(),
        }
    }
}

impl CatalogProvider for TenantScopedCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.inner.schema(name).map(|inner| -> Arc<dyn SchemaProvider> {
            Arc::new(TenantScopedSchemaProvider {
                inner,
                tenant_id: Arc::clone(&self.tenant_id),
            })
        })
    }
}

/// `SchemaProvider` decorator that wraps every returned table.
#[derive(Debug)]
struct TenantScopedSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    tenant_id: Arc<str>,
}

#[async_trait]
impl SchemaProvider for TenantScopedSchemaProvider {
    fn owner_name(&self) -> Option<&str> {
        self.inner.owner_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let Some(inner) = self.inner.table(name).await? else {
            return Ok(None);
        };
        let wrapped = TenantScopedTableProvider::new(inner, &self.tenant_id)?;
        Ok(Some(Arc::new(wrapped)))
    }

    async fn table_type(&self, name: &str) -> DataFusionResult<Option<TableType>> {
        // Cheap path: ask the inner directly so `information_schema` queries
        // don't pay the cost of wrapping every table.
        self.inner.table_type(name).await
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

/// `TableProvider` decorator that enforces `tenant_id = '<t>'` on every
/// `scan()` at the row level AND hides the `tenant_id` column from the
/// schema it advertises to clients.
///
/// The row-level filter prevents leaks regardless of how the inner
/// provider honours pushed-down filters. The hidden column prevents
/// callers from observing, projecting, sorting on, or even referencing
/// the tenancy key — `SELECT tenant_id FROM logs` becomes a plain
/// "column not found" planning error.
#[derive(Debug)]
struct TenantScopedTableProvider {
    inner: Arc<dyn TableProvider>,
    /// Schema visible to clients: the inner schema with `tenant_id`
    /// removed. Held as `Arc` so `schema()` is allocation-free.
    filtered_schema: SchemaRef,
    /// Mapping from a filtered-schema column index to the
    /// corresponding original-schema index. Used to translate
    /// projections before delegating to `inner.scan(...)`.
    index_map: Arc<[usize]>,
    /// Original-schema index of the `tenant_id` column, appended to the
    /// inner projection so the row-level filter can reference it.
    tenant_col_idx: usize,
    /// Pre-built `tenant_id = '<t>'` predicate, reused for both the
    /// pushed-down pruning filter and the row-level `FilterExec`.
    tenant_filter: Expr,
}

impl TenantScopedTableProvider {
    /// Wrap an inner table so every scan is tenant-scoped.
    ///
    /// # Errors
    ///
    /// Returns [`DataFusionError::Internal`] if the inner schema has no
    /// `tenant_id` column. Failing here — at catalog-resolution time —
    /// turns a missing tenancy key into a loud, immediate error instead
    /// of a silent every-scan failure or, worse, an unscoped table.
    fn new(inner: Arc<dyn TableProvider>, tenant_id: &str) -> DataFusionResult<Self> {
        let original = inner.schema();
        let tenant_col_idx = original
            .fields()
            .iter()
            .position(|field| field.name() == COL_TENANT_ID)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "TenantScopedTableProvider: inner schema is missing the `{COL_TENANT_ID}` \
                     column required for tenant isolation"
                ))
            })?;

        let mut fields = Vec::with_capacity(original.fields().len().saturating_sub(1));
        let mut index_map = Vec::with_capacity(original.fields().len().saturating_sub(1));
        for (i, field) in original.fields().iter().enumerate() {
            if i != tenant_col_idx {
                fields.push(Arc::clone(field));
                index_map.push(i);
            }
        }
        let filtered_schema = Arc::new(Schema::new_with_metadata(fields, original.metadata().clone()));
        let tenant_filter = col(COL_TENANT_ID).eq(lit(tenant_id));

        Ok(Self {
            inner,
            filtered_schema,
            index_map: index_map.into(),
            tenant_col_idx,
            tenant_filter,
        })
    }

    /// Translate a projection over the filtered schema into one over the
    /// inner provider's original schema. `None` (= "all visible columns")
    /// expands to every non-`tenant_id` column so the inner batches match
    /// the schema we advertised.
    ///
    /// # Errors
    ///
    /// Returns [`DataFusionError::Internal`] if a projection index is out
    /// of range for the advertised schema — a checked lookup rather than
    /// a panic in the gRPC worker.
    fn translate_projection(&self, projection: Option<&Vec<usize>>) -> DataFusionResult<Vec<usize>> {
        projection.map_or_else(
            || Ok(self.index_map.to_vec()),
            |proj| {
                proj.iter()
                    .map(|&i| {
                        self.index_map.get(i).copied().ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "projection index {i} out of range for {}-column tenant-scoped schema",
                                self.index_map.len()
                            ))
                        })
                    })
                    .collect()
            },
        )
    }
}

#[async_trait]
impl TableProvider for TenantScopedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.filtered_schema)
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        // The hidden column has no defaults visible to clients; refuse
        // to return one even if the inner provider has it.
        if column == COL_TENANT_ID {
            return None;
        }
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Visible columns the caller wants, expressed in the inner
        // provider's original schema (`tenant_id` excluded — it's hidden).
        let mut scan_projection = self.translate_projection(projection)?;
        let visible_len = scan_projection.len();
        // Append `tenant_id` so the row-level filter below can reference
        // it; it occupies the trailing slot of the inner scan's output.
        scan_projection.push(self.tenant_col_idx);

        // Push the tenant predicate down too, so partition / row-group
        // pruning still skips other tenants' files (Iceberg identity
        // partition, Parquet statistics) before any data is read.
        let mut all_filters: Vec<Expr> = Vec::with_capacity(filters.len() + 1);
        all_filters.extend(filters.iter().cloned());
        all_filters.push(self.tenant_filter.clone());

        // NB: do NOT push `limit` into the inner scan. The inner provider
        // may apply a limit *before* our row-level filter runs, which
        // would drop matching rows and return fewer than `limit`. The
        // limit is re-applied above the filter instead.
        let inner_plan = self.inner.scan(state, Some(&scan_projection), &all_filters, None).await?;

        // Enforce the tenant predicate at the row level, independent of
        // how the inner provider honours the pushed-down filter (the
        // merged WAL provider only prunes with it — see the module docs).
        let inner_schema = inner_plan.schema();
        let df_schema = DFSchema::try_from(inner_schema.as_ref().clone())?;
        let predicate = state.create_physical_expr(self.tenant_filter.clone(), &df_schema)?;
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, inner_plan)?);

        // Drop the trailing `tenant_id` column so the output matches the
        // advertised (N-1 column) schema. The visible columns occupy
        // indices `0..visible_len` in the filtered plan.
        let filtered_schema = filtered.schema();
        let projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = (0..visible_len)
            .map(|i| {
                let name = filtered_schema.field(i).name().clone();
                let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&name, i));
                (expr, name)
            })
            .collect();
        let projected: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(projection_exprs, filtered)?);

        // Re-apply the caller's limit above the tenant filter.
        Ok(match limit {
            Some(fetch) => Arc::new(GlobalLimitExec::new(projected, 0, Some(fetch))),
            None => projected,
        })
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Filters arrive expressed in terms of the filtered schema,
        // but they refer to columns by NAME, not by index — and the
        // inner column names are a superset of ours. Delegate so the
        // underlying provider's pushdown capabilities (partition
        // columns, Parquet statistics, etc.) keep working for the
        // caller's WHERE clause.
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        // Deliberately return no statistics. The inner provider's totals
        // and per-column min/max span ALL tenants, so forwarding them
        // would (a) leak another tenant's cardinality / value ranges via
        // `EXPLAIN` and (b) feed the optimizer a row count the injected
        // `tenant_id` filter invalidates. Tenant-scoped statistics aren't
        // available at this layer, so the honest answer is "unknown".
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use icegate_common::schema::COL_TENANT_ID;

    use super::TenantScopedTableProvider;

    /// Build a `MemTable` holding rows for two tenants.
    ///
    /// `MemTable::scan` ignores the filters it is handed (it reports
    /// unsupported pushdown and relies on a parent `FilterExec`) — exactly
    /// the "prunes but does not row-filter" behaviour of the production
    /// WAL provider. That makes it the right stand-in to prove the wrapper
    /// enforces isolation on its own, not via the inner provider.
    fn mixed_tenant_table() -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(COL_TENANT_ID, DataType::Utf8, false),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-b", "tenant-a"])),
                Arc::new(StringArray::from(vec![Some("a1"), Some("b1"), Some("a2")])),
            ],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    #[tokio::test]
    async fn scan_filters_rows_even_when_inner_ignores_filters() {
        let provider = TenantScopedTableProvider::new(mixed_tenant_table(), "tenant-a").unwrap();

        // The advertised schema hides `tenant_id`.
        assert_eq!(provider.schema().fields().len(), 1);
        assert_eq!(provider.schema().field(0).name(), "body");

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();

        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            total, 2,
            "only tenant-a's two rows may survive the wrapper's row filter"
        );

        for batch in &batches {
            assert_eq!(batch.num_columns(), 1, "tenant_id must be projected out");
            let bodies = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..bodies.len() {
                assert!(bodies.value(i).starts_with('a'), "leaked a non-tenant-a row");
            }
        }
    }

    #[tokio::test]
    async fn scan_with_explicit_projection_stays_scoped() {
        let provider = TenantScopedTableProvider::new(mixed_tenant_table(), "tenant-b").unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        // Project the single visible column (`body`).
        let plan = provider.scan(&state, Some(&vec![0]), &[], None).await.unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();

        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1, "tenant-b has exactly one row");
    }

    #[tokio::test]
    async fn new_errors_when_tenant_id_column_absent() {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(StringArray::from(vec![Some("x")]))]).unwrap();
        let inner: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        assert!(TenantScopedTableProvider::new(inner, "tenant-a").is_err());
    }
}
