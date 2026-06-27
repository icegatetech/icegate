//! Custom schema provider for the IceGate namespace.
//!
//! Routes WAL-backed tables (logs, spans, metrics, operations) to
//! [`IcegateTableProvider`] (merged Iceberg + WAL) while delegating other
//! tables to the standard `IcebergStaticTableProvider`.

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

use super::WalQueryConfig;
use super::table::IcegateTableProvider;

/// Tables that have a corresponding WAL topic and therefore use the merged
/// Iceberg + WAL provider. The slice value doubles as the WAL topic name —
/// table name == topic name by convention. Tables not listed here (e.g.
/// `events`, which has no WAL topic) fall through to the standard Iceberg-only
/// provider.
const WAL_MERGED_TABLES: &[&str] = &[
    icegate_common::LOGS_TOPIC,
    icegate_common::SPANS_TOPIC,
    icegate_common::METRICS_TOPIC,
    icegate_common::OPERATIONS_TOPIC,
];

/// Schema provider that substitutes `IcegateTableProvider` for WAL-backed
/// tables (logs, spans, metrics, operations) while using standard Iceberg
/// providers for all other tables.
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
    /// Tables listed in [`WAL_MERGED_TABLES`] are wrapped in
    /// [`IcegateTableProvider`] (merged Iceberg + WAL). All other tables use
    /// `IcebergStaticTableProvider`.
    ///
    /// # Errors
    ///
    /// Returns an error if tables cannot be loaded from the catalog.
    #[tracing::instrument(skip(catalog, wal_reader, wal_config), fields(%namespace))]
    pub(super) async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        wal_reader: Arc<ParquetQueueReader>,
        wal_config: WalQueryConfig,
    ) -> Result<Self, iceberg::Error> {
        let table_idents = catalog.list_tables(&namespace).await?;

        // Load all tables concurrently — each table requires a catalog
        // REST call, so parallelizing cuts wall-clock time significantly.
        // With only a handful of tables, unbounded concurrency is fine.
        let tables_loaded = try_join_all(table_idents.iter().map(|ident| {
            let name = ident.name().to_string();
            let catalog = Arc::clone(&catalog);
            let namespace = namespace.clone();
            let wal_reader = Arc::clone(&wal_reader);
            let wal_config = wal_config.clone();
            async move {
                // Look up the WAL topic in the allowlist; absence routes the
                // table through the standard Iceberg-only provider.
                let provider: Arc<dyn TableProvider> =
                    if let Some(topic) = WAL_MERGED_TABLES.iter().copied().find(|t| *t == name) {
                        // WAL-backed table: use our merged provider
                        let table_ident = iceberg::TableIdent::new(namespace, name.clone());
                        let provider =
                            IcegateTableProvider::try_new(catalog, table_ident, topic, wal_reader, wal_config).await?;
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

#[cfg(test)]
mod tests {
    use super::WAL_MERGED_TABLES;

    /// Guard against accidental drift in the WAL-merged table allowlist.
    /// The dispatch in `IcegateSchemaProvider::try_new` keys off this slice;
    /// removing an entry silently disables WAL merging for that table.
    #[test]
    fn wal_merged_tables_covers_logs_spans_metrics_and_operations() {
        assert!(WAL_MERGED_TABLES.contains(&icegate_common::LOGS_TOPIC));
        assert!(WAL_MERGED_TABLES.contains(&icegate_common::SPANS_TOPIC));
        assert!(WAL_MERGED_TABLES.contains(&icegate_common::METRICS_TOPIC));
        assert!(WAL_MERGED_TABLES.contains(&icegate_common::OPERATIONS_TOPIC));
    }
}
