//! Custom schema provider for the IceGate namespace.
//!
//! Routes the "logs" table to [`IcegateTableProvider`] (merged Iceberg + WAL)
//! while delegating other tables to standard `IcebergStaticTableProvider`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use icegate_queue::ParquetQueueReader;

use super::table::IcegateTableProvider;

/// Schema provider that substitutes `IcegateTableProvider` for the "logs" table
/// while using standard Iceberg providers for all other tables.
pub(super) struct IcegateSchemaProvider {
    /// All tables in the namespace, keyed by name.
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl std::fmt::Debug for IcegateSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcegateSchemaProvider")
            .field("tables", &self.tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl IcegateSchemaProvider {
    /// Creates a new schema provider for the given namespace.
    ///
    /// For the "logs" table, creates an `IcegateTableProvider` that merges
    /// Iceberg + WAL data. All other tables use `IcebergStaticTableProvider`.
    ///
    /// # Errors
    ///
    /// Returns an error if tables cannot be loaded from the catalog.
    #[tracing::instrument(skip(catalog, wal_reader), fields(%namespace))]
    pub(super) async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        wal_reader: Arc<ParquetQueueReader>,
    ) -> Result<Self, iceberg::Error> {
        let table_idents = catalog.list_tables(&namespace).await?;

        // Load all tables concurrently — each table requires a catalog
        // REST call, so parallelizing cuts wall-clock time significantly.
        // With only 4 tables, unbounded concurrency is fine.
        let tables_loaded = try_join_all(table_idents.iter().map(|ident| {
            let name = ident.name().to_string();
            let catalog = Arc::clone(&catalog);
            let namespace = namespace.clone();
            let wal_reader = Arc::clone(&wal_reader);
            async move {
                let provider: Arc<dyn TableProvider> = if name == icegate_common::LOGS_TOPIC {
                    // Logs table: use our merged provider
                    let table_ident = iceberg::TableIdent::new(namespace, name.clone());
                    let provider = IcegateTableProvider::try_new(catalog, table_ident, wal_reader).await?;
                    Arc::new(provider)
                } else {
                    // Other tables: standard Iceberg static provider
                    let table = catalog.load_table(ident).await?;
                    let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
                    Arc::new(provider)
                };
                Ok::<_, iceberg::Error>((name, provider))
            }
        }))
        .await?;

        let tables: HashMap<String, Arc<dyn TableProvider>> = tables_loaded.into_iter().collect();

        tracing::debug!(table_count = tables.len(), "Schema provider initialized");
        Ok(Self { tables })
    }
}

#[async_trait]
impl SchemaProvider for IcegateSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.get(name).cloned())
    }
}
