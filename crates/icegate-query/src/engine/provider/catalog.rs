//! Custom catalog provider that routes the IceGate namespace to our
//! WAL-merged schema provider.
//!
//! For the "icegate" namespace, uses [`IcegateSchemaProvider`] which
//! merges Iceberg + WAL data for the logs table. Other namespaces use
//! standard `IcebergSchemaProvider` via `IcebergStaticTableProvider`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use object_store::ObjectStore;

use super::schema::IcegateSchemaProvider;

/// The namespace that contains IceGate tables (logs, spans, events, metrics).
const ICEGATE_NAMESPACE: &str = "icegate";

/// Catalog provider that substitutes our custom schema provider for the
/// IceGate namespace while using standard Iceberg providers for all others.
#[derive(Debug)]
pub struct IcegateCatalogProvider {
    /// Schema providers keyed by namespace name.
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl IcegateCatalogProvider {
    /// Creates a new catalog provider.
    ///
    /// Lists all namespaces from the catalog. For the "icegate" namespace,
    /// creates an `IcegateSchemaProvider` (WAL-merged). For all other
    /// namespaces, creates standard schema providers with
    /// `IcebergStaticTableProvider`.
    ///
    /// # Errors
    ///
    /// Returns an error if namespaces or tables cannot be loaded.
    #[tracing::instrument(skip(catalog, wal_store))]
    pub async fn try_new(
        catalog: Arc<dyn Catalog>,
        wal_store_url: ObjectStoreUrl,
        wal_store: Arc<dyn ObjectStore>,
        wal_base_path: String,
    ) -> Result<Self, iceberg::Error> {
        let namespace_idents = catalog.list_namespaces(None).await?;

        // Flatten multi-level namespace idents into single names
        // (IceGate uses single-level namespaces like "icegate")
        let namespace_names: Vec<String> = namespace_idents.iter().flat_map(|ns| ns.as_ref().clone()).collect();

        let mut schemas: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::with_capacity(namespace_names.len());

        for name in &namespace_names {
            let ns_ident = NamespaceIdent::new(name.clone());

            if name == ICEGATE_NAMESPACE {
                // Use our custom WAL-merged schema provider
                let provider = IcegateSchemaProvider::try_new(
                    Arc::clone(&catalog),
                    ns_ident,
                    wal_store_url.clone(),
                    Arc::clone(&wal_store),
                    wal_base_path.clone(),
                )
                .await?;
                schemas.insert(name.clone(), Arc::new(provider));
            } else {
                // Standard schema provider with static table providers
                let provider = build_standard_schema_provider(Arc::clone(&catalog), ns_ident).await?;
                schemas.insert(name.clone(), Arc::new(provider));
            }
        }

        tracing::debug!(namespace_count = schemas.len(), "Catalog provider initialized");
        Ok(Self { schemas })
    }
}

impl CatalogProvider for IcegateCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

/// Build a standard schema provider using `IcebergStaticTableProvider` for
/// all tables in the given namespace.
///
/// This is the non-WAL path, used for namespaces other than "icegate".
async fn build_standard_schema_provider(
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
) -> Result<StandardSchemaProvider, iceberg::Error> {
    let table_idents = catalog.list_tables(&namespace).await?;

    let tables_loaded = try_join_all(table_idents.iter().map(|ident| {
        let catalog = Arc::clone(&catalog);
        async move {
            let table = catalog.load_table(ident).await?;
            let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
            Ok::<_, iceberg::Error>((
                ident.name().to_string(),
                Arc::new(provider) as Arc<dyn datafusion::datasource::TableProvider>,
            ))
        }
    }))
    .await?;

    let tables: HashMap<String, Arc<dyn datafusion::datasource::TableProvider>> = tables_loaded.into_iter().collect();

    Ok(StandardSchemaProvider { tables })
}

/// Simple schema provider wrapping static Iceberg table providers.
#[derive(Debug)]
struct StandardSchemaProvider {
    tables: HashMap<String, Arc<dyn datafusion::datasource::TableProvider>>,
}

#[async_trait::async_trait]
impl SchemaProvider for StandardSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn datafusion::datasource::TableProvider>>> {
        Ok(self.tables.get(name).cloned())
    }
}
