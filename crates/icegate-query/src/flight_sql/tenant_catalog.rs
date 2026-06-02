//! Tenant-scoped catalog wrappers that force-inject `tenant_id = '<t>'`
//! into every scan, regardless of the SQL path used to reach the table.
//!
//! ## Why this exists
//!
//! The session provider previously hid the raw `iceberg` catalog after
//! creating tenant-filtered SQL views. That approach worked but was a
//! perimeter defence: a single missed lookup path (a future Flight SQL
//! method, an analyzer rule, a custom catalog probe) could leak rows
//! across tenants. This module flips the model to defence-in-depth:
//! the `iceberg.icegate.<table>` paths are kept fully visible, but the
//! `TableProvider`s returned for them are wrapped so that every
//! `scan()` call automatically AND'es a tenant predicate into the
//! filter list before delegating.
//!
//! The wrapper composes with DataFusion's existing filter-pushdown
//! machinery: the underlying Iceberg provider sees the injected
//! `tenant_id = '<t>'` filter exactly as if the client had typed it,
//! prunes partitions/row-groups accordingly, and never returns rows
//! belonging to other tenants. The injected filter is added inside
//! `scan()` after pushdown analysis has finished, so it cannot be
//! observed or stripped by DataFusion's planner.
//!
//! ## Hiding `tenant_id` from clients
//!
//! On top of injecting the predicate, the wrapper also removes the
//! `tenant_id` column from the schema it advertises. Clients see an
//! N-1 column table with no notion of tenancy: `SELECT *`,
//! `DESCRIBE`, `information_schema.columns`, and Flight SQL
//! `get_tables(include_schema=true)` all reflect the filtered schema.
//! This removes the entire surface for "leak my own tenant id back to
//! me" foot-guns and prevents clients from accidentally sorting,
//! grouping, or joining on what is effectively an internal partition
//! key.
//!
//! Projection indices passed to `scan()` are translated through a
//! pre-computed `filtered_idx -> original_idx` map so the underlying
//! Iceberg provider continues to see its original column layout.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType, col, lit};
use datafusion::physical_plan::ExecutionPlan;
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
        Ok(Some(Arc::new(TenantScopedTableProvider::new(
            inner,
            Arc::clone(&self.tenant_id),
        ))))
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

/// `TableProvider` decorator that injects `tenant_id = '<t>'` into every
/// `scan()` call AND hides the `tenant_id` column from the schema it
/// advertises to clients.
///
/// The injected filter prevents row-level leaks. The hidden column
/// prevents callers from observing, projecting, sorting on, or even
/// referencing the tenancy key — `SELECT tenant_id FROM logs` becomes
/// a plain "column not found" planning error.
#[derive(Debug)]
struct TenantScopedTableProvider {
    inner: Arc<dyn TableProvider>,
    tenant_id: Arc<str>,
    /// Schema visible to clients: the inner schema with `tenant_id`
    /// removed. Held as `Arc` so `schema()` is allocation-free.
    filtered_schema: SchemaRef,
    /// Mapping from a filtered-schema column index to the
    /// corresponding original-schema index. Used to translate
    /// projections before delegating to `inner.scan(...)`.
    index_map: Arc<[usize]>,
}

impl TenantScopedTableProvider {
    fn new(inner: Arc<dyn TableProvider>, tenant_id: Arc<str>) -> Self {
        let original = inner.schema();
        let mut fields = Vec::with_capacity(original.fields().len());
        let mut index_map = Vec::with_capacity(original.fields().len());
        for (i, field) in original.fields().iter().enumerate() {
            if field.name() != COL_TENANT_ID {
                fields.push(Arc::clone(field));
                index_map.push(i);
            }
        }
        let filtered_schema = Arc::new(Schema::new_with_metadata(fields, original.metadata().clone()));
        Self {
            inner,
            tenant_id,
            filtered_schema,
            index_map: index_map.into(),
        }
    }

    /// Translate a projection over the filtered schema into one over
    /// the inner provider's original schema. When the caller omits
    /// projection (`None` = "all visible columns"), we still pass an
    /// explicit projection downstream that excludes `tenant_id` so the
    /// inner provider's batches match the schema we advertised.
    fn translate_projection(&self, projection: Option<&Vec<usize>>) -> Vec<usize> {
        projection.map_or_else(
            || self.index_map.to_vec(),
            |proj| proj.iter().map(|&i| self.index_map[i]).collect(),
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
        // Build the tenant predicate and append it to whatever pushdown
        // filters DataFusion has already decided to send. The underlying
        // Iceberg provider reports `Exact` pushdown for `tenant_id`
        // (it's a partition column), so this prunes manifests / data
        // files before any row-group is even opened. The filter is
        // expressed in terms of the *inner* column name, which still
        // exists in the underlying provider's schema even though we
        // hide it from clients.
        let tenant_filter = col(COL_TENANT_ID).eq(lit(self.tenant_id.as_ref()));
        let mut all_filters: Vec<Expr> = Vec::with_capacity(filters.len() + 1);
        all_filters.extend(filters.iter().cloned());
        all_filters.push(tenant_filter);

        let inner_projection = self.translate_projection(projection);
        self.inner.scan(state, Some(&inner_projection), &all_filters, limit).await
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
        // Per-column statistics from the inner provider are positional
        // over the original schema, which still carries `tenant_id`.
        // Re-project them through the same `filtered_idx -> original_idx`
        // map that `scan()` uses so the surviving column stats line up
        // with the N-1 column schema we advertise and the `tenant_id`
        // entry is dropped. Row and byte totals are column-agnostic and
        // pass through unchanged; the tenant predicate injected in
        // `scan()` refines the row count at execution time.
        let mut stats = self.inner.statistics()?;
        // `Statistics::project` indexes `column_statistics` positionally
        // and panics on an out-of-bounds index, so only re-project when
        // the inner provider emitted a full per-column vector aligned
        // with its schema. The common "row count only" case carries an
        // empty vector: keep the totals but drop the (absent) column
        // stats rather than risk a positional mismatch or a panic.
        if stats.column_statistics.len() == self.inner.schema().fields().len() {
            Some(stats.project(Some(&self.index_map.to_vec())))
        } else {
            stats.column_statistics = Vec::new();
            Some(stats)
        }
    }
}
