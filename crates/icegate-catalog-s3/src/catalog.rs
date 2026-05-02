//! S3-backed implementation of [`iceberg::Catalog`].

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit,
    TableCreation, TableIdent,
};

use crate::config::S3CatalogConfig;
use crate::error::{Error, Result};
use crate::model::{CatalogRoot, PreparedTableCommit, TableEntry, TableKey, TableMetadataState};
use crate::storage::CatalogStorage;
use crate::storage::s3::S3CatalogStorage;

/// S3 catalog implementation based on atomic compare-and-swap updates of the catalog root.
pub struct S3Catalog {
    storage: Arc<dyn CatalogStorage>,
    file_io: FileIO,
    tables_uri_prefix: String,
}

impl fmt::Debug for S3Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Catalog").finish_non_exhaustive()
    }
}

impl S3Catalog {
    /// Create a new S3 catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if S3 configuration is invalid or storage cannot be created.
    // Async is intentional: the constructor follows the async initialization pattern used by callers.
    #[allow(clippy::unused_async)]
    pub async fn new(config: S3CatalogConfig, file_io: FileIO) -> Result<Self> {
        let tables_uri_prefix = Self::compute_tables_uri_prefix(&config);
        let storage = S3CatalogStorage::new(&config)?;
        Ok(Self::with_storage(Arc::new(storage), file_io, tables_uri_prefix))
    }

    /// Create a catalog with externally provided storage implementation.
    pub(crate) fn with_storage(storage: Arc<dyn CatalogStorage>, file_io: FileIO, tables_uri_prefix: String) -> Self {
        Self {
            storage,
            file_io,
            tables_uri_prefix,
        }
    }

    fn compute_tables_uri_prefix(config: &S3CatalogConfig) -> String {
        let warehouse = config.warehouse.trim_matches('/');
        if warehouse.is_empty() {
            format!("s3://{}/catalog/tables", config.bucket)
        } else {
            format!("s3://{}/{}/catalog/tables", config.bucket, warehouse)
        }
    }

    /// Multi-table commit with one root compare-and-swap update.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CommitConflict`] when root or table state changed concurrently.
    pub async fn commit_transaction(&self, commits: Vec<TableCommit>) -> Result<Vec<Table>> {
        if commits.is_empty() {
            return Ok(Vec::new());
        }

        // Load a stable base for requirement validation and metadata building.
        // A second load just before the CAS uses the freshest version token and
        // reduces the conflict window.
        let (mut base_root, version) = self.storage.load_root().await?;
        let staged = self.stage_transaction_commits(&mut base_root, commits).await?;
        self.storage.save_root(base_root, &version).await?;

        staged
            .into_iter()
            .map(|commit| {
                Table::builder()
                    .identifier(commit.identifier)
                    .metadata_location(commit.metadata_location)
                    .metadata(commit.metadata)
                    .file_io(self.file_io.clone())
                    .build()
                    .map_err(Error::from)
            })
            .collect()
    }

    async fn stage_transaction_commits(
        &self,
        root: &mut CatalogRoot,
        commits: Vec<TableCommit>,
    ) -> Result<Vec<PreparedTableCommit>> {
        let mut staged = Vec::with_capacity(commits.len());
        let mut seen_keys = HashSet::with_capacity(commits.len());

        for mut commit in commits {
            let identifier = commit.identifier().clone();
            let key = TableKey::from_ident(&identifier);
            if !seen_keys.insert(key.clone()) {
                return Err(Error::CommitConflict);
            }
            let entry = root.get_active(&key).ok_or_else(|| Error::TableNotFound(identifier.clone()))?;
            let metadata_location = entry.metadata_location().to_string();

            let current_metadata = self.storage.read_table_metadata(&metadata_location).await?;
            let staged_commit = TableMetadataState::new(metadata_location, current_metadata).prepare_commit(
                identifier,
                key,
                commit.take_requirements(),
                commit.take_updates(),
            )?;

            // TODO(med): first we need to check the conditions, and then write the metadata. Otherwise, there will be extra garbage.
            root.apply_commit(&staged_commit.key, staged_commit.metadata_location.clone())?;
            // Metadata is intentionally written before the root CAS update.
            // If CAS fails, this file becomes an orphan and can be cleaned up by GC later.
            self.storage
                .write_table_metadata(&staged_commit.metadata_location, &staged_commit.metadata)
                .await?;
            staged.push(staged_commit);
        }

        Ok(staged)
    }
}

#[async_trait]
impl Catalog for S3Catalog {
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>) -> IcebergResult<Vec<NamespaceIdent>> {
        let result: Result<Vec<NamespaceIdent>> = async {
            let (root, _) = self.storage.load_root().await?;
            Ok(root.list_namespaces(parent))
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        let result: Result<Namespace> = async {
            let (mut root, version) = self.storage.load_root().await?;
            if !root.create_namespace(namespace, properties.clone()) {
                return Err(Error::Iceberg(IcebergError::new(
                    ErrorKind::NamespaceAlreadyExists,
                    format!("Namespace already exists: {namespace}"),
                )));
            }

            self.storage.save_root(root, &version).await?;
            Ok(Namespace::with_properties(namespace.clone(), properties))
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        let result: Result<Namespace> = async {
            let (root, _) = self.storage.load_root().await?;
            let Some(entry) = root.get_namespace(namespace) else {
                return Err(Error::NamespaceNotFound(namespace.clone()));
            };

            Ok(Namespace::with_properties(
                namespace.clone(),
                entry.properties().clone(),
            ))
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        let result: Result<bool> = async {
            let (root, _) = self.storage.load_root().await?;
            Ok(root.get_namespace(namespace).is_some())
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        let result: Result<()> = async {
            let (mut root, version) = self.storage.load_root().await?;
            if !root.update_namespace(namespace, properties) {
                return Err(Error::NamespaceNotFound(namespace.clone()));
            }

            self.storage.save_root(root, &version).await
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    /// Drop an existing namespace.
    ///
    /// The namespace must be empty: no tables and no descendant namespaces.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        let result: Result<()> = async {
            let (mut root, version) = self.storage.load_root().await?;
            root.drop_namespace(namespace)?;
            self.storage.save_root(root, &version).await
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        let result: Result<Vec<TableIdent>> = async {
            let (root, _) = self.storage.load_root().await?;
            Ok(root.active_tables_in_namespace(namespace))
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    /// Create and register new table
    async fn create_table(&self, namespace: &NamespaceIdent, creation: TableCreation) -> IcebergResult<Table> {
        let result: Result<Table> = async {
            let (mut latest_root, version) = self.storage.load_root().await?;
            let (state, table_ident) = latest_root.create_table(namespace, creation, &self.tables_uri_prefix)?;

            self.storage
                .write_table_metadata(state.metadata_location(), state.metadata())
                .await?;
            self.storage.save_root(latest_root, &version).await?;

            Table::builder()
                .identifier(table_ident)
                .metadata_location(state.metadata_location().to_owned())
                .metadata(state.metadata().clone())
                .file_io(self.file_io.clone())
                .build()
                .map_err(Error::from)
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let result: Result<Table> = async {
            let (root, _) = self.storage.load_root().await?;
            let key = TableKey::from_ident(table);
            let entry = root.get_active(&key).ok_or_else(|| Error::TableNotFound(table.clone()))?;
            let metadata = self.storage.read_table_metadata(entry.metadata_location()).await?;

            Table::builder()
                .identifier(table.clone())
                .metadata_location(entry.metadata_location().to_string())
                .metadata(metadata)
                .file_io(self.file_io.clone())
                .build()
                .map_err(Error::from)
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let result: Result<()> = async {
            let (mut latest_root, version) = self.storage.load_root().await?;
            let key = TableKey::from_ident(table);
            latest_root.tombstone(&key)?;
            self.storage.save_root(latest_root, &version).await
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let result: Result<bool> = async {
            let (root, _) = self.storage.load_root().await?;
            Ok(root.get_active(&TableKey::from_ident(table)).is_some())
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        let result: Result<()> = async {
            let (mut latest_root, version) = self.storage.load_root().await?;
            let src_key = TableKey::from_ident(src);
            let dst_key = TableKey::from_ident(dest);
            latest_root.rename(&src_key, dst_key)?;
            self.storage.save_root(latest_root, &version).await
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    /// Register table - link existing table to Catalog
    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> IcebergResult<Table> {
        let result: Result<Table> = async {
            let metadata = self.storage.read_table_metadata(&metadata_location).await?;
            let table_id = metadata.uuid().to_string();

            let (mut latest_root, version) = self.storage.load_root().await?;
            let key = TableKey::from_ident(table);
            latest_root.link_table(key, TableEntry::new(table_id, metadata_location.clone()))?;

            self.storage.save_root(latest_root, &version).await?;

            Table::builder()
                .identifier(table.clone())
                .metadata_location(metadata_location)
                .metadata(metadata)
                .file_io(self.file_io.clone())
                .build()
                .map_err(Error::from)
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        let response = self
            .commit_transaction(vec![commit])
            .await
            .map_err(IcebergError::from)?
            .into_iter()
            .next();
        let Some(response) = response else {
            unreachable!("commit_transaction with one request must return one response");
        };

        Ok(response)
    }
}
