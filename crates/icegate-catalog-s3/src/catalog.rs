//! S3-backed implementation of [`iceberg::Catalog`].

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit,
    TableCreation, TableIdent, TableRequirement, TableUpdate,
};
use uuid::Uuid;

use crate::config::S3CatalogConfig;
use crate::error::{Error, Result, StorageError};
use crate::model::{CatalogRoot, TableEntry, TableStatus, split_table_key, table_key};
use crate::storage::CatalogStorage;
use crate::storage::s3::S3CatalogStorage;

/// Request payload for transactional multi-table commit.
#[derive(Debug, Clone)]
pub struct CommitTableRequest {
    /// Table identifier to update.
    pub identifier: TableIdent,
    /// Requirements that must hold before applying updates.
    pub requirements: Vec<TableRequirement>,
    /// Metadata updates to apply.
    pub updates: Vec<TableUpdate>,
}

/// Response for transactional multi-table commit.
#[derive(Debug, Clone)]
pub struct CommitTableResponse {
    /// Updated table identifier.
    pub identifier: TableIdent,
    /// New metadata location URI.
    pub metadata_location: String,
    /// Updated table metadata.
    pub metadata: TableMetadata,
}

#[derive(Debug, Clone)]
struct StagedCommit {
    identifier: TableIdent,
    key: String,
    table_id: String,
    expected_metadata_location: String,
    metadata_location: String,
    sequence_number: i64,
    metadata: TableMetadata,
}

/// S3 catalog implementation based on atomic compare-and-swap updates of the catalog root.
pub struct S3Catalog {
    storage: Arc<dyn CatalogStorage>,
    file_io: FileIO,
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
    #[allow(clippy::unused_async)]
    pub async fn new(config: S3CatalogConfig, file_io: FileIO) -> Result<Self> {
        let storage = S3CatalogStorage::new(&config)?;
        Ok(Self::with_storage(Arc::new(storage), file_io))
    }

    // TODO(crit): add CatalogStorage to `new` constructor
    pub(crate) fn with_storage(storage: Arc<dyn CatalogStorage>, file_io: FileIO) -> Self {
        Self { storage, file_io }
    }

    /// Multi-table commit with one root compare-and-swap update.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CommitConflict`] when root or table state changed concurrently.
    pub async fn commit_transaction(&self, commits: Vec<CommitTableRequest>) -> Result<Vec<CommitTableResponse>> {
        // TODO(crit): зачем CommitTableRequest? Кажется, можно использовать TableCommit.
        // TODO(crit): этот метод нужен только для API, в iceberg-rust Catalog нет метода для коммита на несколько таблиц. Может быть и не стоит сейчас это реализовывать
        if commits.is_empty() {
            return Ok(Vec::new());
        }

        let (base_root, _) = self.storage.load_root().await?;
        let staged = self.stage_transaction_commits(base_root, commits).await?;

        let (mut latest_root, version) = self.storage.load_root().await?;
        for commit in &staged {
            // TODO(crit): all this block must be in root
            let entry = latest_root.tables.get_mut(&commit.key).ok_or(Error::CommitConflict)?;
            if entry.status != TableStatus::Active
                || entry.table_id != commit.table_id
                || entry.metadata_location != commit.expected_metadata_location
            {
                return Err(Error::CommitConflict);
            }
            entry.metadata_location.clone_from(&commit.metadata_location);
            entry.commit = commit.sequence_number;
        }

        // TODO(crit): add retries and merge. кажется, надо разделить мерж на 2 ветки: таблицы, изменяемые в текущей мутации не изменились на S3 и таблицы уже кто-то поменял. Во втором варианте нужно мержить саму мету (как?). Или может ставить лок на таблицы перед commit.
        self.storage.save_root(latest_root, &version).await?;

        Ok(staged
            .into_iter()
            .map(|commit| CommitTableResponse { // TODO(crit): маппинг убрать в CommitTableResponse
                identifier: commit.identifier,
                metadata_location: commit.metadata_location,
                metadata: commit.metadata,
            })
            .collect())
    }

    async fn stage_transaction_commits(
        &self,
        root: CatalogRoot,
        commits: Vec<CommitTableRequest>,
    ) -> Result<Vec<StagedCommit>> {
        let mut staged = Vec::with_capacity(commits.len());

        for request in commits {
            let key = table_key(&request.identifier);
            let entry = root
                .tables
                .get(&key)
                .filter(|entry| entry.status == TableStatus::Active)
                .ok_or_else(|| Error::TableNotFound(request.identifier.clone()))?
                .clone();

            let metadata = self
                .build_next_metadata(
                    &request.identifier,
                    &entry.metadata_location,
                    request.requirements,
                    request.updates,
                )
                .await?;
            let metadata_location = self
                .storage
                .write_table_metadata(&entry.table_id, metadata.last_sequence_number(), &metadata)
                .await?;

            // TODO(crit): что тут происходит? Мы физически пишем мету в S3?
            staged.push(StagedCommit {
                identifier: request.identifier,
                key,
                table_id: entry.table_id,
                expected_metadata_location: entry.metadata_location,
                metadata_location,
                sequence_number: metadata.last_sequence_number(),
                metadata,
            });
        }

        Ok(staged)
    }

    async fn build_next_metadata(
        &self,
        table_ident: &TableIdent,
        metadata_location: &str,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<TableMetadata> {
        // TODO(crit): затратная операция с чтением текущей меты и приминением изменений.
        let current = self.storage.read_table_metadata(metadata_location).await?;

        for requirement in requirements {
            requirement.check(Some(&current))?;
        }

        let mut builder = current.into_builder(Some(metadata_location.to_string()));
        for update in updates {
            builder = update.apply(builder)?;
        }

        let build = builder.build()?;
        if build.metadata.location().is_empty() {
            return Err(Error::InvalidMetadata(format!(
                "empty table location for {table_ident}"
            )));
        }

        Ok(build.metadata)
    }

    async fn load_active_table_entry(&self, ident: &TableIdent) -> Result<TableEntry> {
        let (root, _) = self.storage.load_root().await?;
        let key = table_key(ident);
        root.tables
            .get(&key)
            .filter(|entry| entry.status == TableStatus::Active)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(ident.clone()))
    }
}

#[async_trait]
impl Catalog for S3Catalog {
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>) -> IcebergResult<Vec<NamespaceIdent>> {
        let result: Result<Vec<NamespaceIdent>> = async {
            let (root, _) = self.storage.load_root().await?;
            // TODO(crit): think about to store BTreeSet, cause method list_namespaces used in all namespase methods. Может быть хранить сразу в BTree, снизу также надо, например, листить по namespace.
            let mut full_namespaces: BTreeSet<NamespaceIdent> = BTreeSet::new();

            for key in root.tables.keys() {
                let Some((namespace, _)) = split_table_key(key) else {
                    continue;
                };

                let mut parts = Vec::new();
                for part in namespace.iter() {
                    parts.push(part.clone());
                    if let Ok(prefix) = NamespaceIdent::from_vec(parts.clone()) {
                        full_namespaces.insert(prefix);
                    }
                }
            }

            let selected = match parent {
                None => full_namespaces
                    .into_iter()
                    .filter_map(|ns| ns.first().cloned())
                    .map(NamespaceIdent::new)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
                Some(parent_namespace) => {
                    let parent_parts = parent_namespace.as_ref();
                    full_namespaces
                        .into_iter()
                        .filter_map(|ns| {
                            let parts = ns.as_ref();
                            if parts.len() <= parent_parts.len() {
                                return None;
                            }
                            if &parts[..parent_parts.len()] != parent_parts {
                                return None;
                            }
                            let mut child = parent_parts.clone();
                            child.push(parts[parent_parts.len()].clone());
                            NamespaceIdent::from_vec(child).ok()
                        })
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect()
                }
            };

            Ok(selected)
        }
        .await;

        result.map_err(map_error)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        if self.namespace_exists(namespace).await? {
            return Err(IcebergError::new(
                ErrorKind::NamespaceAlreadyExists,
                format!("Namespace already exists: {namespace}"),
            ));
        }

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        if self.namespace_exists(namespace).await? {
            return Ok(Namespace::new(namespace.clone()));
        }

        Err(map_error(Error::NamespaceNotFound(namespace.clone())))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        // TODO(crit): list_namespaces used twice
        let namespaces = self.list_namespaces(None).await?;
        if namespaces.contains(namespace) {
            return Ok(true);
        }

        let mut stack = namespaces;
        while let Some(parent) = stack.pop() {
            let children = self.list_namespaces(Some(&parent)).await?;
            if children.contains(namespace) {
                return Ok(true);
            }
            stack.extend(children);
        }

        Ok(false)
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        if self.namespace_exists(namespace).await? {
            return Ok(());
        }

        Err(map_error(Error::NamespaceNotFound(namespace.clone())))
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        let tables = self.list_tables(namespace).await?;
        if !tables.is_empty() { // TODO(crit): точно ли нужно такое условие?
            return Err(IcebergError::new(
                ErrorKind::PreconditionFailed,
                format!("Namespace {namespace} is not empty"),
            ));
        }

        if self.namespace_exists(namespace).await? {
            return Ok(());
        }

        Err(map_error(Error::NamespaceNotFound(namespace.clone())))
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        let result: Result<Vec<TableIdent>> = async {
            let (root, _) = self.storage.load_root().await?;
            let tables = root
                .tables
                .iter()
                .filter_map(|(key, entry)| {
                    if entry.status != TableStatus::Active { // TODO(crit): это должен быть метод доменной сущности
                        return None;
                    }
                    let (key_namespace, table_name) = split_table_key(key)?;
                    if &key_namespace != namespace {
                        return None;
                    }
                    Some(TableIdent::new(key_namespace, table_name))
                })
                .collect();
            Ok(tables)
        }
        .await;

        result.map_err(map_error)
    }

    async fn create_table(&self, namespace: &NamespaceIdent, creation: TableCreation) -> IcebergResult<Table> {
        let result: Result<Table> = async {
            let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());
            let table_id = Uuid::new_v4().to_string();
            let table_location = creation
                .location
                .clone()
                .unwrap_or_else(|| self.storage.default_table_location(&table_id));

            let creation = TableCreation {
                location: Some(table_location),
                ..creation
            };

            let metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?.metadata;
            let metadata_location = self
                .storage
                .write_table_metadata(&table_id, metadata.last_sequence_number(), &metadata)
                .await?;

            let (mut latest_root, version) = self.storage.load_root().await?;
            let key = table_key(&table_ident);
            if latest_root // TODO(crit): нужен метод в root
                .tables
                .get(&key)
                .is_some_and(|entry| entry.status == TableStatus::Active)
            {
                return Err(Error::TableAlreadyExists(table_ident));
            }

            latest_root.tables.insert( // TODO(crit): нужен метод в root
                key,
                TableEntry {// TODO(crit): конструктор
                    table_id,
                    status: TableStatus::Active,
                    metadata_location: metadata_location.clone(),
                    commit: metadata.last_sequence_number(),
                },
            );

            self.storage.save_root(latest_root, &version).await?;

            Table::builder()
                .identifier(table_ident)
                .metadata_location(metadata_location)
                .metadata(metadata)
                .file_io(self.file_io.clone())
                .build()
                .map_err(Error::from)
        }
        .await;

        result.map_err(map_error)
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let result: Result<Table> = async {
            let (root, _) = self.storage.load_root().await?;
            let key = table_key(table);
            let entry = root
                .tables
                .get(&key)
                .filter(|entry| entry.status == TableStatus::Active)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            let metadata = self.storage.read_table_metadata(&entry.metadata_location).await?;

            Table::builder()
                .identifier(table.clone())
                .metadata_location(entry.metadata_location.clone())
                .metadata(metadata)
                .file_io(self.file_io.clone())
                .build()
                .map_err(Error::from)
        }
        .await;

        result.map_err(map_error)
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let result: Result<()> = async {
            let (mut latest_root, version) = self.storage.load_root().await?;
            let key = table_key(table);
            let entry = latest_root
                .tables
                .get_mut(&key)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;

            if entry.status != TableStatus::Active {
                return Err(Error::TableNotFound(table.clone()));
            }

            entry.status = TableStatus::Tombstoned;
            self.storage.save_root(latest_root, &version).await
        }
        .await;

        result.map_err(map_error)
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let result: Result<bool> = async {
            let (root, _) = self.storage.load_root().await?;
            Ok(root
                .tables
                .get(&table_key(table))
                .is_some_and(|entry| entry.status == TableStatus::Active))
        }
        .await;

        result.map_err(map_error)
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        let result: Result<()> = async {
            let (mut latest_root, version) = self.storage.load_root().await?;
            let src_key = table_key(src);
            let dst_key = table_key(dest);

            if latest_root
                .tables
                .get(&dst_key)
                .is_some_and(|entry| entry.status == TableStatus::Active)
            {
                return Err(Error::TableAlreadyExists(dest.clone()));
            }

            let source = latest_root
                .tables
                .remove(&src_key)
                .ok_or_else(|| Error::TableNotFound(src.clone()))?;

            if source.status != TableStatus::Active {
                return Err(Error::TableNotFound(src.clone()));
            }

            latest_root.tables.insert(dst_key, source);
            self.storage.save_root(latest_root, &version).await
        }
        .await;

        result.map_err(map_error)
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> IcebergResult<Table> {
        let result: Result<Table> = async {
            let metadata = self.storage.read_table_metadata(&metadata_location).await?;

            let (mut latest_root, version) = self.storage.load_root().await?;
            let key = table_key(table);
            if latest_root
                .tables
                .get(&key)
                .is_some_and(|entry| entry.status == TableStatus::Active)
            {
                return Err(Error::TableAlreadyExists(table.clone()));
            }

            latest_root.tables.insert(
                key,
                TableEntry {
                    table_id: Uuid::new_v4().to_string(),
                    status: TableStatus::Active,
                    metadata_location: metadata_location.clone(),
                    commit: metadata.last_sequence_number(),
                },
            );

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

        result.map_err(map_error)
    }

    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        // TODO(crit): этот метод надо попробовать объединить с commit_transaction, кажется, можно просто вызывать commit_transaction
        let table_ident = commit.identifier().clone();
        let requirements = commit.take_requirements();
        let updates = commit.take_updates();

        let staged_result: Result<StagedCommit> = async {
            let base = self.load_active_table_entry(&table_ident).await?;
            let metadata = self
                .build_next_metadata(&table_ident, &base.metadata_location, requirements, updates)
                .await?;
            let metadata_location = self
                .storage
                .write_table_metadata(&base.table_id, metadata.last_sequence_number(), &metadata)
                .await?;

            Ok(StagedCommit {
                identifier: table_ident.clone(),
                key: table_key(&table_ident),
                table_id: base.table_id,
                expected_metadata_location: base.metadata_location,
                metadata_location,
                sequence_number: metadata.last_sequence_number(),
                metadata,
            })
        }
        .await;

        let staged = match staged_result {
            Ok(staged) => staged,
            Err(error) => return Err(map_error(error)),
        };

        let update_result: Result<Table> = async {
            let (mut latest_root, version) = self.storage.load_root().await?;
            let entry = latest_root.tables.get_mut(&staged.key).ok_or(Error::CommitConflict)?;

            if entry.status != TableStatus::Active
                || entry.table_id != staged.table_id
                || entry.metadata_location != staged.expected_metadata_location
            {
                return Err(Error::CommitConflict);
            }

            entry.metadata_location.clone_from(&staged.metadata_location);
            entry.commit = staged.sequence_number;
            self.storage.save_root(latest_root, &version).await?;

            Table::builder()
                .identifier(table_ident)
                .metadata_location(staged.metadata_location)
                .metadata(staged.metadata)
                .file_io(self.file_io.clone())
                .build()
                .map_err(Error::from)
        }
        .await;

        update_result.map_err(map_error)
    }
}

fn map_error(error: Error) -> IcebergError {
    // TODO(crit): убрать в Error
    match error {
        Error::Iceberg(iceberg_error) => iceberg_error,
        Error::Storage(StorageError::NotFound(path)) => {
            IcebergError::new(ErrorKind::Unexpected, format!("Object not found: {path}"))
        }
        Error::Storage(StorageError::PreconditionFailed) => {
            IcebergError::new(ErrorKind::CatalogCommitConflicts, "CAS precondition failed").with_retryable(true)
        }
        Error::Storage(StorageError::AlreadyExists(path)) => IcebergError::new(
            ErrorKind::CatalogCommitConflicts,
            format!("Object already exists: {path}"),
        )
        .with_retryable(true),
        Error::Storage(StorageError::Io(message)) => IcebergError::new(ErrorKind::Unexpected, message),
        Error::TableNotFound(table) => IcebergError::new(ErrorKind::TableNotFound, format!("Table not found: {table}")),
        Error::TableAlreadyExists(table) => {
            IcebergError::new(ErrorKind::TableAlreadyExists, format!("Table already exists: {table}"))
        }
        Error::NamespaceNotFound(namespace) => IcebergError::new(
            ErrorKind::NamespaceNotFound,
            format!("Namespace not found: {namespace}"),
        ),
        Error::CommitConflict => {
            IcebergError::new(ErrorKind::CatalogCommitConflicts, "catalog commit conflict").with_retryable(true)
        }
        Error::InvalidMetadata(message) => IcebergError::new(ErrorKind::DataInvalid, message),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    // TODO(crit): убрать использование методов для тестов в s3 storage

    use std::collections::HashMap;
    use std::sync::Arc;

    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::Credentials;
    use bytes::Bytes;
    use iceberg::io::FileIO;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, TableCreation, TableRequirement, TableUpdate};
    use object_store::path::Path;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::minio::MinIO;

    use super::*;
    use crate::codec::json::JsonCatalogCodec;
    use crate::storage::in_memory::InMemoryCatalogStorage;
    use crate::storage::s3::S3CatalogStorage;

    const MINIO_USER: &str = "minioadmin";
    const MINIO_PASSWORD: &str = "minioadmin";

    struct TestEnv {
        _minio: testcontainers::ContainerAsync<MinIO>,
        catalog: S3Catalog,
        s3_storage: Arc<S3CatalogStorage>,
    }

    async fn make_catalog() -> TestEnv {
        let minio = MinIO::default().start().await.expect("start minio");
        let port = minio.get_host_port_ipv4(9000).await.expect("minio port");
        let endpoint = format!("http://127.0.0.1:{port}");
        let bucket = format!("catalog-{}", Uuid::new_v4());

        let cfg = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .endpoint_url(endpoint.clone())
            .credentials_provider(Credentials::new(MINIO_USER, MINIO_PASSWORD, None, None, "test"))
            .load()
            .await;
        let client = aws_sdk_s3::Client::new(&cfg);
        client.create_bucket().bucket(&bucket).send().await.expect("create bucket");

        let file_io = FileIO::new_with_memory();
        let storage = Arc::new(
            S3CatalogStorage::new(&S3CatalogConfig {
                bucket: bucket.clone(),
                region: "us-east-1".to_string(),
                endpoint: Some(endpoint),
                access_key_id: Some(MINIO_USER.to_string()),
                secret_access_key: Some(MINIO_PASSWORD.to_string()),
                warehouse: "warehouse".to_string(),
                codec: crate::CatalogCodecKind::Json,
            })
            .expect("storage"),
        );

        let catalog = S3Catalog::with_storage(storage.clone(), file_io);

        TestEnv {
            _minio: minio,
            catalog,
            s3_storage: storage,
        }
    }

    fn make_in_memory_catalog() -> S3Catalog {
        S3Catalog::with_storage(
            Arc::new(InMemoryCatalogStorage::new(Arc::new(JsonCatalogCodec))),
            FileIO::new_with_memory(),
        )
    }

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema")
    }

    async fn create_table(env: &S3Catalog, namespace: &NamespaceIdent, name: &str) -> Table {
        env.create_table(
            namespace,
            TableCreation::builder().name(name.to_string()).schema(test_schema()).build(),
        )
        .await
        .expect("create table")
    }

    fn update_request(table: &Table, key: &str, value: &str) -> CommitTableRequest {
        CommitTableRequest {
            identifier: table.identifier().clone(),
            requirements: vec![TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            }],
            updates: vec![TableUpdate::SetProperties {
                updates: HashMap::from([(key.to_string(), value.to_string())]),
            }],
        }
    }

    #[tokio::test]
    async fn create_and_load_table_with_in_memory_storage() {
        let catalog = make_in_memory_catalog();
        let namespace = NamespaceIdent::new("ns1".to_string());

        let created = create_table(&catalog, &namespace, "tbl").await;
        let loaded = catalog.load_table(created.identifier()).await.expect("load table");

        assert_eq!(loaded.identifier(), created.identifier());
        assert_eq!(loaded.metadata_location(), created.metadata_location());
    }

    #[tokio::test]
    async fn rename_with_in_memory_storage_preserves_table() {
        let catalog = make_in_memory_catalog();
        let namespace = NamespaceIdent::new("ns1".to_string());
        let created = create_table(&catalog, &namespace, "tbl").await;
        let renamed = TableIdent::new(namespace, "tbl-renamed".to_string());

        catalog
            .rename_table(created.identifier(), &renamed)
            .await
            .expect("rename table");

        assert!(!catalog.table_exists(created.identifier()).await.expect("source exists"));
        assert!(catalog.table_exists(&renamed).await.expect("renamed exists"));
    }

    #[tokio::test]
    async fn create_vs_create() {
        let env = make_catalog().await;
        let ns = NamespaceIdent::new("ns1".to_string());

        let task1 = env.catalog.create_table(
            &ns,
            TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
        );
        let task2 = env.catalog.create_table(
            &ns,
            TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
        );

        let (r1, r2) = tokio::join!(task1, task2);
        assert!(r1.is_ok() ^ r2.is_ok());
    }

    #[tokio::test]
    async fn commit_vs_commit() {
        let env = make_catalog().await;
        let ns = NamespaceIdent::new("ns1".to_string());
        let table = create_table(&env.catalog, &ns, "tbl").await;

        let commit1 = env.catalog.commit_transaction(vec![update_request(&table, "k", "v1")]);
        let commit2 = env.catalog.commit_transaction(vec![update_request(&table, "k", "v2")]);

        let (r1, r2) = tokio::join!(commit1, commit2);
        assert!(r1.is_ok() ^ r2.is_ok());
    }

    #[tokio::test]
    async fn commit_vs_drop() {
        let env = make_catalog().await;
        let ns = NamespaceIdent::new("ns1".to_string());
        let table = create_table(&env.catalog, &ns, "tbl").await;

        let commit = env.catalog.commit_transaction(vec![update_request(&table, "k", "v")]);
        let drop_op = env.catalog.drop_table(table.identifier());

        let (r1, r2) = tokio::join!(commit, drop_op);
        assert!(r1.is_ok() ^ r2.is_ok());
    }

    #[tokio::test]
    async fn rename_vs_rename() {
        let env = make_catalog().await;
        let ns = NamespaceIdent::new("ns1".to_string());
        let table = create_table(&env.catalog, &ns, "tbl").await;

        let dest1 = TableIdent::new(ns.clone(), "tbl_a".to_string());
        let dest2 = TableIdent::new(ns.clone(), "tbl_b".to_string());

        let r1 = env.catalog.rename_table(table.identifier(), &dest1);
        let r2 = env.catalog.rename_table(table.identifier(), &dest2);

        let (v1, v2) = tokio::join!(r1, r2);
        assert!(v1.is_ok() ^ v2.is_ok());
    }

    #[tokio::test]
    async fn rename_vs_drop() {
        let env = make_catalog().await;
        let ns = NamespaceIdent::new("ns1".to_string());
        let table = create_table(&env.catalog, &ns, "tbl").await;
        let dest = TableIdent::new(ns, "tbl_renamed".to_string());

        let rename = env.catalog.rename_table(table.identifier(), &dest);
        let drop_op = env.catalog.drop_table(table.identifier());

        let (r1, r2) = tokio::join!(rename, drop_op);
        assert!(r1.is_ok() ^ r2.is_ok());
    }

    #[tokio::test]
    async fn create_vs_drop_same_name() {
        let env = make_catalog().await;
        let ns = NamespaceIdent::new("ns1".to_string());
        let table = create_table(&env.catalog, &ns, "tbl").await;
        env.catalog.drop_table(table.identifier()).await.expect("drop table");

        let recreated = env
            .catalog
            .create_table(
                &ns,
                TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
            )
            .await;

        assert!(recreated.is_ok());
    }

    #[tokio::test]
    async fn tx_ab_vs_single_a() {
        let env = make_catalog().await;
        let ns = NamespaceIdent::new("ns1".to_string());
        let a = create_table(&env.catalog, &ns, "a").await;
        let b = create_table(&env.catalog, &ns, "b").await;

        let tx_ab = env
            .catalog
            .commit_transaction(vec![update_request(&a, "k", "v1"), update_request(&b, "k", "v1")]);
        let single_a = env.catalog.commit_transaction(vec![update_request(&a, "k", "v2")]);

        let (r1, r2) = tokio::join!(tx_ab, single_a);
        assert!(r1.is_ok() ^ r2.is_ok());
    }

    #[tokio::test]
    async fn tx_ab_vs_tx_ac() {
        let env = make_catalog().await;
        let ns = NamespaceIdent::new("ns1".to_string());
        let a = create_table(&env.catalog, &ns, "a").await;
        let b = create_table(&env.catalog, &ns, "b").await;
        let c = create_table(&env.catalog, &ns, "c").await;

        let tx_ab = env
            .catalog
            .commit_transaction(vec![update_request(&a, "k", "ab"), update_request(&b, "k", "ab")]);
        let tx_ac = env
            .catalog
            .commit_transaction(vec![update_request(&a, "k", "ac"), update_request(&c, "k", "ac")]);

        let (r1, r2) = tokio::join!(tx_ab, tx_ac);
        assert!(r1.is_ok() ^ r2.is_ok());
    }

    #[tokio::test]
    async fn orphan_metadata() {
        let env = make_catalog().await;
        let ns = NamespaceIdent::new("ns1".to_string());

        let orphan_path = env.s3_storage.metadata_path_for("orphan", 1);
        env.s3_storage
            .put_unconditional_for_test(&orphan_path, Bytes::from_static(b"{}"))
            .await
            .expect("write orphan");
        env.s3_storage.head(&orphan_path).await.expect("orphan metadata exists");
        env.s3_storage
            .list_prefix(&Path::from("warehouse/catalog/tables/orphan"))
            .await
            .expect("list orphan prefix");

        let table = create_table(&env.catalog, &ns, "tbl").await;
        let loaded = env.catalog.load_table(table.identifier()).await;
        assert!(loaded.is_ok());
    }
}
