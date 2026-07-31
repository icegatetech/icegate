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
//! (needed to evaluate the row-level filter), in ascending schema order;
//! a [`ProjectionExec`] then drops `tenant_id` and restores the caller's
//! column order, so the batches handed back match the advertised schema
//! exactly.
//!
//! ## Why the inner projection is sorted
//!
//! Asking in schema order is load-bearing, not tidiness. A
//! `TableProvider` may emit a projection in *schema* order rather than the
//! order it was asked for: the Iceberg reader relabels the batch instead
//! of reordering it whenever the two orders differ but the projected
//! columns line up positionally by Arrow type and nullability. A wrapper
//! that appended `tenant_id` last would then evaluate the tenant
//! predicate against whichever column landed in that slot, and every row
//! would silently disappear — `SELECT name FROM spans` returned nothing
//! while `count(*)` returned the full row count, because `name` and
//! `tenant_id` are both non-nullable strings. Requesting ascending order
//! leaves no provider anything to reorder.

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
use icegate_common::GLOBAL_TABLES;
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
        // Global reference tables carry no `tenant_id` by design and hold
        // identical rows for every tenant, so there is nothing to scope. This is
        // an explicit allowlist and never an inferred "has no tenant_id, so skip
        // wrapping" rule — the inferred form would make any future table that
        // forgets the column silently readable across tenants. Everything absent
        // from the list still goes through the decorator, which fails closed.
        if GLOBAL_TABLES.contains(&name) {
            return Ok(Some(inner));
        }
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
    /// Original-schema index of the `tenant_id` column, merged into the
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
        // Visible columns the caller wants, in the caller's order,
        // expressed in the inner provider's original schema (`tenant_id`
        // excluded — it's hidden).
        let visible_columns = self.translate_projection(projection)?;
        // Add `tenant_id` so the row-level filter below can reference it,
        // then sort: the inner scan must be asked in schema order (see the
        // module docs), never in the caller's order.
        let mut scan_projection = visible_columns.clone();
        scan_projection.push(self.tenant_col_idx);
        scan_projection.sort_unstable();

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

        // Drop `tenant_id` and put the visible columns back in the caller's
        // order, so the output matches the advertised (N-1 column) schema.
        // Each column's slot in the sorted inner output is found by its
        // original-schema index.
        let scan_schema = filtered.schema();
        let projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = visible_columns
            .iter()
            .map(|original_idx| {
                let slot = scan_projection.binary_search(original_idx).map_err(|_| {
                    DataFusionError::Internal(format!(
                        "column {original_idx} is missing from the tenant-scoped scan projection"
                    ))
                })?;
                let name = scan_schema.field(slot).name().clone();
                let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&name, slot));
                Ok((expr, name))
            })
            .collect::<DataFusionResult<_>>()?;
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
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::any::Any;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
    use datafusion::catalog::{MemorySchemaProvider, SchemaProvider, Session, TableProvider};
    use datafusion::datasource::MemTable;
    use datafusion::error::Result as DataFusionResult;
    use datafusion::logical_expr::{Expr, TableType};
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use icegate_common::schema::COL_TENANT_ID;
    use icegate_common::{GLOBAL_TABLES, LOGS_TABLE, PRICES_TABLE};

    use super::{TenantScopedSchemaProvider, TenantScopedTableProvider};

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

    /// A table with the given columns and no rows.
    fn empty_table(columns: &[&str]) -> Arc<MemTable> {
        let fields: Vec<Field> = columns.iter().map(|c| Field::new(*c, DataType::Utf8, true)).collect();
        let schema = Arc::new(Schema::new(fields));
        Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap())
    }

    fn provider_with(name: &str, columns: &[&str]) -> TenantScopedSchemaProvider {
        let inner = MemorySchemaProvider::new();
        inner.register_table(name.to_string(), empty_table(columns)).unwrap();
        TenantScopedSchemaProvider {
            inner: Arc::new(inner),
            tenant_id: Arc::from("tenant-alpha"),
        }
    }

    #[test]
    fn prices_is_on_the_global_allowlist() {
        assert!(GLOBAL_TABLES.contains(&PRICES_TABLE));
        assert!(!GLOBAL_TABLES.contains(&LOGS_TABLE));
    }

    #[tokio::test]
    async fn global_table_passes_through_unwrapped() {
        // `prices` has no tenant_id and must still resolve — its schema is
        // returned intact, not stripped of a column it never had.
        let provider = provider_with(PRICES_TABLE, &["provider", "model"]);
        let table = provider.table(PRICES_TABLE).await.unwrap().expect("prices resolves");
        assert_eq!(table.schema().fields().len(), 2);
        assert!(table.schema().field_with_name("provider").is_ok());
    }

    #[tokio::test]
    async fn tenant_table_is_wrapped_and_hides_tenant_id() {
        let provider = provider_with(LOGS_TABLE, &["tenant_id", "body"]);
        let table = provider.table(LOGS_TABLE).await.unwrap().expect("logs resolves");
        // The decorator projects tenant_id back out of the advertised schema.
        assert_eq!(table.schema().fields().len(), 1);
        assert!(table.schema().field_with_name("tenant_id").is_err());
    }

    /// Inner provider that emits projected columns in SCHEMA order while
    /// advertising them in the order it was asked for.
    ///
    /// This is the production Iceberg reader's behaviour: when the requested
    /// order differs from schema order but the projected columns line up
    /// positionally by Arrow type and nullability, it relabels the batch
    /// instead of reordering it. A caller that asks in schema order never
    /// meets the case, because the two orders then coincide.
    #[derive(Debug)]
    struct SchemaOrderTableProvider {
        schema: SchemaRef,
        batch: RecordBatch,
        /// Projections received, in call order, so a test can assert what the
        /// wrapper *asked for* and not only what came back.
        requests: Mutex<Vec<Vec<usize>>>,
    }

    #[async_trait]
    impl TableProvider for SchemaOrderTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            let requested: Vec<usize> =
                projection.cloned().unwrap_or_else(|| (0..self.schema.fields().len()).collect());
            self.requests.lock().unwrap().push(requested.clone());

            let mut schema_order = requested.clone();
            schema_order.sort_unstable();
            let columns = schema_order.iter().map(|&i| Arc::clone(self.batch.column(i))).collect();

            let advertised = Arc::new(self.schema.project(&requested)?);
            let options = RecordBatchOptions::new().with_match_field_names(false);
            let batch = RecordBatch::try_new_with_options(Arc::clone(&advertised), columns, &options)?;
            MemTable::try_new(advertised, vec![vec![batch]])?
                .scan(state, None, &[], None)
                .await
        }
    }

    /// Rows for two tenants where every column is, like `tenant_id`, a
    /// non-nullable string.
    ///
    /// That uniformity is the point: it is exactly the condition under which
    /// the reader relabels rather than reorders, so any permutation of these
    /// columns is indistinguishable from the right one by Arrow type alone.
    /// `spans` has this shape — `name` and `tenant_id` are both required
    /// strings — and it is the only column of `spans` that reproduced the
    /// defect.
    fn spans_shaped_table() -> Arc<SchemaOrderTableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(COL_TENANT_ID, DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("level", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-b", "tenant-a"])),
                Arc::new(StringArray::from(vec!["lookup", "insert", "commit"])),
                Arc::new(StringArray::from(vec!["debug", "warn", "info"])),
            ],
        )
        .unwrap();
        Arc::new(SchemaOrderTableProvider {
            schema,
            batch,
            requests: Mutex::new(Vec::new()),
        })
    }

    /// Values of one string column across all batches, in row order.
    fn string_column(batches: &[RecordBatch], index: usize) -> Vec<Option<String>> {
        batches
            .iter()
            .flat_map(|batch| {
                let array = batch.column(index).as_any().downcast_ref::<StringArray>().unwrap();
                (0..array.len())
                    .map(|i| array.is_valid(i).then(|| array.value(i).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn projecting_a_lone_non_nullable_string_column_returns_its_rows() {
        // Regression: the wrapper appended `tenant_id` *after* the visible
        // columns, so the inner scan was asked for [name, tenant_id] — an
        // order the Iceberg reader does not honour. The tenant filter then
        // read `name` as the tenancy key and dropped every row, which is why
        // `SELECT name FROM spans` came back empty while `count(*)` reported
        // the full table.
        let inner = spans_shaped_table();
        let provider =
            TenantScopedTableProvider::new(Arc::clone(&inner) as Arc<dyn TableProvider>, "tenant-a").unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, Some(&vec![0]), &[], None).await.unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();

        assert_eq!(
            string_column(&batches, 0),
            vec![Some("lookup".to_string()), Some("commit".to_string())],
            "projecting `name` alone must return tenant-a's two rows, never an empty result"
        );
    }

    #[tokio::test]
    async fn inner_scan_is_asked_for_columns_in_ascending_schema_order() {
        // The wrapper must not require the inner provider to reorder columns.
        let inner = spans_shaped_table();
        let provider =
            TenantScopedTableProvider::new(Arc::clone(&inner) as Arc<dyn TableProvider>, "tenant-a").unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        // Visible schema is [name, level]; ask for them back to front.
        provider.scan(&state, Some(&vec![1, 0]), &[], None).await.unwrap();

        let requests = inner.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "one scan must issue exactly one inner projection");
        assert_eq!(
            requests[0],
            vec![0, 1, 2],
            "inner projection must list tenant_id plus the visible columns in ascending schema order"
        );
    }

    #[tokio::test]
    async fn scan_output_follows_the_caller_projection_order() {
        // Sorting the inner projection must not leak into the output: the
        // caller still gets its columns in the order it asked for.
        let inner = spans_shaped_table();
        let provider =
            TenantScopedTableProvider::new(Arc::clone(&inner) as Arc<dyn TableProvider>, "tenant-a").unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, Some(&vec![1, 0]), &[], None).await.unwrap();
        assert_eq!(
            plan.schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>(),
            vec!["level".to_string(), "name".to_string()]
        );

        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(
            string_column(&batches, 0),
            vec![Some("debug".to_string()), Some("info".to_string())]
        );
        assert_eq!(
            string_column(&batches, 1),
            vec![Some("lookup".to_string()), Some("commit".to_string())]
        );
    }

    #[tokio::test]
    async fn non_allowlisted_table_without_tenant_id_still_fails_closed() {
        // The regression that matters: a future table that forgets tenant_id
        // must error, not silently become readable by every tenant.
        let provider = provider_with("some_new_table", &["a", "b"]);
        let result = provider.table("some_new_table").await;
        assert!(
            result.is_err(),
            "a tenant-less table off the allowlist must not resolve"
        );
    }
}
