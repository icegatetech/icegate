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
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::S3CatalogConfig;
use crate::error::{Error, Result, StorageError};
use crate::infra::retrier::Retrier;
use crate::model::{
    CatalogRoot, CatalogTableLink, IcebergTableMetadata, MergeTransactionResult, PreparedCommitRef, TableId, TableKey,
    TableMetadataLocation, TableUpdate,
};
use crate::storage::cached::CachedCatalogStorage;
use crate::storage::s3::S3CatalogStorage;
use crate::storage::{CatalogStorage, LoadOutcome, Version};

/// Per-table state carried across retry rounds inside `commit_transaction`.
///
/// Lives in `catalog.rs` because the orchestration loop owns these. The domain
/// layer never sees this type — `commit_transaction` projects each entry into a
/// [`PreparedCommitRef`] before calling [`CatalogRoot::merge_transaction`].
pub(crate) struct PendingCommit {
    pub(crate) identifier: TableIdent,
    pub(crate) key: TableKey,
    pub(crate) requirements: Vec<iceberg::TableRequirement>,
    pub(crate) updates: Vec<iceberg::TableUpdate>,
    /// `None` until a metadata file has been built and written for this table
    /// on top of the *currently observed* head. Reset to `None` whenever the
    /// table moves underneath us, forcing the next round to prepare metadata again.
    pub(crate) table: Option<TableUpdate>,
}

/// S3 catalog implementation based on atomic compare-and-swap updates of the catalog root.
pub struct S3Catalog {
    storage: Arc<dyn CatalogStorage>,
    file_io: FileIO,
    tables_uri_prefix: String,
    retrier: Retrier,
    cancel_token: CancellationToken,
}

impl fmt::Debug for S3Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Catalog").finish_non_exhaustive()
    }
}

impl S3Catalog {
    /// Create a new S3 catalog.
    ///
    /// `cancel_token` propagates external shutdown into retry loops and storage
    /// operations. Pass [`CancellationToken::new()`] when the catalog has no
    /// upstream lifecycle to honour.
    ///
    /// # Errors
    ///
    /// Returns an error if S3 configuration is invalid or storage cannot be created.
    // Async is intentional: the constructor follows the async initialization pattern used by callers.
    #[allow(clippy::unused_async)]
    pub async fn new(config: S3CatalogConfig, file_io: FileIO, cancel_token: CancellationToken) -> Result<Self> {
        let tables_uri_prefix = Self::compute_tables_uri_prefix(&config);
        // Catalog layer drives optimistic CAS loops; storage keeps the I/O
        // curve. Two distinct backoff shapes for two distinct retry semantics.
        let retrier = Retrier::new(config.cas_retrier_config.clone());
        let enable_cache = config.enable_cache;
        let metadata_cache_cap = config.metadata_cache_cap;
        let raw_storage: Arc<dyn CatalogStorage> = Arc::new(S3CatalogStorage::new(&config, cancel_token.clone())?);
        let storage: Arc<dyn CatalogStorage> = if enable_cache {
            Arc::new(CachedCatalogStorage::new(raw_storage, metadata_cache_cap))
        } else {
            raw_storage
        };
        Ok(Self::with_storage(
            storage,
            file_io,
            tables_uri_prefix,
            retrier,
            cancel_token,
        ))
    }

    /// Create a catalog with externally provided storage implementation.
    pub(crate) fn with_storage(
        storage: Arc<dyn CatalogStorage>,
        file_io: FileIO,
        tables_uri_prefix: String,
        retrier: Retrier,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            storage,
            file_io,
            tables_uri_prefix,
            retrier,
            cancel_token,
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

    /// Load the current root as an owned value plus its CAS version, for the
    /// mutate-then-publish paths ([`Self::update_root`], [`Self::commit_transaction`]).
    /// The absent case is normalised into an empty default root.
    ///
    /// Owned because the caller mutates the root and re-wraps it in a fresh
    /// `Arc` for `save_root`; the version is the token that CAS-guards that
    /// write. Contrast [`Self::load_root_for_read`], which returns a shared
    /// `Arc` with no version for borrow-only callers.
    ///
    /// Associated (not a method) so the retry closures — which capture only the
    /// storage `Arc`, not `self` — can share it. An unconditional load must
    /// never yield `NotModified`; that is a storage-contract violation, surfaced
    /// as an I/O error.
    async fn load_root_for_mutation(storage: &Arc<dyn CatalogStorage>) -> Result<(CatalogRoot, Version)> {
        match storage.load_root(None).await? {
            LoadOutcome::Loaded { root, version } => Ok((Arc::unwrap_or_clone(root), version)),
            LoadOutcome::Absent => Ok((CatalogRoot::default(), Version::Absent)),
            LoadOutcome::NotModified => Err(Error::Storage(StorageError::Io(
                "storage returned NotModified for unconditional load".to_string(),
            ))),
        }
    }

    /// Load the current root as a shared [`Arc`] for borrow-only readers. The
    /// absent case is normalised into an empty default root.
    ///
    /// Returns no CAS version because reads never publish. Sharing the `Arc`
    /// avoids the deep copy that [`Self::load_root_for_mutation`] pays via
    /// `Arc::unwrap_or_clone`: with the cache holding its own reference, that
    /// clone would duplicate the entire catalog (all namespaces, tables,
    /// tombstones) on every read — wasteful on the hot query path where the
    /// caller only borrows one field.
    ///
    /// An unconditional load must never yield `NotModified`; that is a
    /// storage-contract violation, surfaced as an I/O error.
    async fn load_root_for_read(storage: &Arc<dyn CatalogStorage>) -> Result<Arc<CatalogRoot>> {
        match storage.load_root(None).await? {
            LoadOutcome::Loaded { root, .. } => Ok(root),
            LoadOutcome::Absent => Ok(Arc::new(CatalogRoot::default())),
            LoadOutcome::NotModified => Err(Error::Storage(StorageError::Io(
                "storage returned NotModified for unconditional load".to_string(),
            ))),
        }
    }

    /// Run an in-memory mutation of the catalog root and CAS-publish it.
    ///
    /// `op` is replayed against a freshly loaded root on every attempt, so it
    /// must be a pure function of the root. A CAS conflict (another writer
    /// published first) or a transient storage fault reloads and replays — one
    /// round-trip per attempt, with no separate conflict-time reload.
    ///
    /// Lost-response convergence is `op`'s responsibility: when our own write
    /// physically landed but the ack was lost (surfacing as a conflict),
    /// replaying against the new root must recognise that already-applied effect
    /// and return success rather than erroring. See `create_table` and
    /// `register_table` for the "same identity ⇒ Ok, foreign occupant ⇒ Err"
    /// pattern — both keyed on a stable token. `create_namespace` deliberately
    /// opts out (no such token; it stays strict — see its doc).
    async fn update_root<F, T>(&self, op: F) -> Result<T>
    where
        F: Fn(&mut CatalogRoot) -> Result<T> + Send + Sync,
        T: Send + Sync,
    {
        let op = Arc::new(op);
        // The closure return stays `()` to fit the retrier's shape; the
        // successful `op` value is parked here and taken out after the loop.
        let result_slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));

        self.retrier
            .retry(
                {
                    let storage = Arc::clone(&self.storage);
                    let op = Arc::clone(&op);
                    let result_slot = Arc::clone(&result_slot);
                    move || {
                        let storage = Arc::clone(&storage);
                        let op = Arc::clone(&op);
                        let result_slot = Arc::clone(&result_slot);
                        async move {
                            let (mut root, version) = Self::load_root_for_mutation(&storage).await?;

                            let value = op(&mut root)?;

                            match storage.save_root(Arc::new(root), &version).await {
                                Ok(_) => {
                                    *result_slot.lock().await = Some(value);
                                    Ok((false, ()))
                                }
                                // Transient storage faults are exhausted inside the
                                // storage layer (its own backoff curve), so a CAS
                                // conflict is the only retry signal that reaches here.
                                Err(error) if error.is_conflict() => Ok((true, ())),
                                Err(error) => Err(error),
                            }
                        }
                    }
                },
                &self.cancel_token,
            )
            .await?;

        let mut slot = result_slot.lock().await;
        slot.take().ok_or(Error::CasMaxAttempts)
    }

    /// Multi-table commit with one root compare-and-swap update.
    ///
    /// Drives a two-level optimistic retry loop:
    ///
    /// 1. **Root CAS conflict** — another writer published a different root in
    ///    parallel but the tables we touch have not moved. We re-merge our
    ///    already prepared metadata pointers onto the new root and retry the CAS
    ///    without re-writing metadata files.
    /// 2. **Table-state conflict** — another writer published a new metadata
    ///    version for one of our tables. We drop the stale prepared commit, read
    ///    the new current metadata, re-validate `TableRequirement`s against it,
    ///    write a fresh metadata file (N+2), and retry.
    ///
    /// Logical requirement failures (e.g., `AssertRefSnapshotId`) are not
    /// retried — they propagate as [`Error::Iceberg`].
    ///
    /// Each retry round may leave orphaned metadata files behind. Cleanup is
    /// tracked under the existing GC TODO (historical note); high-conflict
    /// workloads should run GC periodically.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CasMaxAttempts`] when the retry budget is exhausted,
    /// [`Error::Cancelled`] on shutdown, or the underlying error for any
    /// non-conflict, non-transient failure.
    pub async fn commit_transaction(&self, commits: Vec<TableCommit>) -> Result<Vec<Table>> {
        if commits.is_empty() {
            return Ok(Vec::new());
        }

        let mut initial_commits: Vec<PendingCommit> = Vec::with_capacity(commits.len());
        let mut seen_keys = HashSet::with_capacity(commits.len());
        for mut commit in commits {
            let identifier = commit.identifier().clone();
            let key = TableKey::from_ident(&identifier);
            if !seen_keys.insert(key.clone()) {
                return Err(Error::CommitConflict);
            }
            initial_commits.push(PendingCommit {
                identifier,
                key,
                requirements: commit.take_requirements(),
                updates: commit.take_updates(),
                table: None,
            });
        }

        let commits: Arc<Mutex<Vec<PendingCommit>>> = Arc::new(Mutex::new(initial_commits));
        let storage = Arc::clone(&self.storage);
        let file_io = self.file_io.clone();
        let result_slot: Arc<Mutex<Option<Vec<Table>>>> = Arc::new(Mutex::new(None));

        self.retrier
            .retry(
                {
                    let commits = Arc::clone(&commits);
                    let storage = Arc::clone(&storage);
                    let file_io = file_io.clone();
                    let result_slot = Arc::clone(&result_slot);
                    move || {
                        let commits = Arc::clone(&commits);
                        let storage = Arc::clone(&storage);
                        let file_io = file_io.clone();
                        let result_slot = Arc::clone(&result_slot);
                        async move {
                            let (mut root, version) = Self::load_root_for_mutation(&storage).await?;

                            let mut locked_commits = commits.lock().await;

                            // Step 1: build metadata files for any commit whose
                            // prepared table slot is empty (first attempt or rebuilt in
                            // a previous attempt after a table-state conflict).
                            Self::prepare_commits(&storage, &mut locked_commits, &root).await?;

                            // Step 2: merge prepared pointers onto the freshly
                            // loaded root. Stale prepared entries (head moved
                            // underneath us) come back in `rebuild` — reset
                            // them to `None` and let the next attempt rebuild.
                            //
                            // Project the orchestration-owned `PendingCommit`s
                            // into domain refs in a scope that ends the immutable
                            // borrow of `guard` before the `iter_mut` below.
                            let merge = {
                                let refs: Vec<PreparedCommitRef<'_>> = locked_commits
                                    .iter()
                                    .map(|commit| PreparedCommitRef {
                                        key: &commit.key,
                                        table: commit.table.as_ref(),
                                    })
                                    .collect();
                                root.merge_transaction(&refs)?
                            };
                            match merge {
                                MergeTransactionResult::NeedRebuild(outcome) => {
                                    for key in outcome.rebuild {
                                        if let Some(commit) = locked_commits.iter_mut().find(|c| c.key == key) {
                                            commit.table = None;
                                        }
                                    }
                                    return Ok((true, ()));
                                }
                                MergeTransactionResult::Ready => {}
                            }

                            // Step 3: build the response tables from prepared
                            // state *before* the CAS. The build is a pure,
                            // in-memory projection (no I/O, no dependency on the
                            // saved root), so doing it first leaves no fallible
                            // step between a durable `save_root` and the `Ok`
                            // return — a committed root must never surface as an
                            // `Err`. On a root CAS conflict these are discarded
                            // and the next attempt rebuilds them after re-merging.
                            let tables = Self::build_tables(&locked_commits, &file_io)?;

                            // Step 4: CAS-publish the merged root. On conflict
                            // we keep prepared metadata intact — the next attempt only
                            // re-merges them against the new root. Transient
                            // storage faults are exhausted inside the storage
                            // layer, so a conflict is the only retry signal here.
                            match storage.save_root(Arc::new(root), &version).await {
                                Ok(_) => {
                                    drop(locked_commits);
                                    *result_slot.lock().await = Some(tables);
                                    Ok((false, ()))
                                }
                                Err(error) if error.is_conflict() => Ok((true, ())),
                                Err(error) => Err(error),
                            }
                        }
                    }
                },
                &self.cancel_token,
            )
            .await?;

        let mut slot = result_slot.lock().await;
        slot.take().ok_or(Error::CasMaxAttempts)
    }

    async fn prepare_commits(
        storage: &Arc<dyn CatalogStorage>,
        commits: &mut [PendingCommit],
        root: &CatalogRoot,
    ) -> Result<()> {
        for commit in commits.iter_mut() {
            if commit.table.is_some() {
                continue;
            }

            let entry = root
                .get_active(&commit.key)
                .ok_or_else(|| Error::TableNotFound(commit.identifier.clone()))?;
            let persisted = entry.clone();
            let current_location = entry.metadata_location().clone();
            let current_metadata = storage.read_table_metadata(current_location.as_str()).await?;
            let table = IcebergTableMetadata::new(current_location, Arc::unwrap_or_clone(current_metadata))
                .prepare_commit(
                    commit.identifier.clone(),
                    persisted,
                    commit.requirements.clone(),
                    commit.updates.clone(),
                )?;
            storage
                .write_table_metadata(table.updated.metadata_location().as_str(), table.updated.metadata())
                .await?;
            commit.table = Some(table);
        }
        Ok(())
    }

    fn build_tables(commits: &[PendingCommit], file_io: &FileIO) -> Result<Vec<Table>> {
        commits
            .iter()
            .map(|commit| {
                let table = commit
                    .table
                    .as_ref()
                    .ok_or_else(|| Error::InvalidMetadata("prepared commit missing on success path".to_string()))?;
                Table::builder()
                    .identifier(table.full_name.clone())
                    .metadata_location(table.updated.metadata_location().as_str().to_string())
                    .metadata(table.updated.metadata().clone())
                    .file_io(file_io.clone())
                    .build()
                    .map_err(Error::from)
            })
            .collect()
    }
}

#[async_trait]
impl Catalog for S3Catalog {
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>) -> IcebergResult<Vec<NamespaceIdent>> {
        let result: Result<Vec<NamespaceIdent>> = async {
            let root = Self::load_root_for_read(&self.storage).await?;
            Ok(root.list_namespaces(parent))
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    /// Create a namespace, failing if it already exists.
    ///
    /// Strict per the Iceberg contract: an existing namespace — even with
    /// identical properties — yields [`ErrorKind::NamespaceAlreadyExists`].
    ///
    /// Unlike `create_table` / `register_table`, this op does **not** perform
    /// lost-response convergence. Those have a stable identity token (the table
    /// UUID, generated once outside the retry loop) that distinguishes "my own
    /// write landed" from "a foreign/duplicate write". A namespace has no such
    /// token — its only identity is the mutable `properties` — so converging on
    /// "same properties" would silently accept ordinary duplicates, not just
    /// self-retries, breaking the contract. The rare cost is that a genuine
    /// lost ack (our CAS physically landed but the response was lost) surfaces
    /// as a spurious `NamespaceAlreadyExists`; namespaces are created once at
    /// setup, so this is tolerable. Callers that want create-if-absent must
    /// guard with `namespace_exists` (see `icegate-maintain`'s migrate path).
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        let namespace_owned = namespace.clone();
        let properties_owned = properties;

        let result: Result<Namespace> = self
            .update_root(move |root| {
                if !root.create_namespace(&namespace_owned, properties_owned.clone()) {
                    return Err(Error::Iceberg(IcebergError::new(
                        ErrorKind::NamespaceAlreadyExists,
                        format!("Namespace already exists: {namespace_owned}"),
                    )));
                }
                Ok(Namespace::with_properties(
                    namespace_owned.clone(),
                    properties_owned.clone(),
                ))
            })
            .await;

        result.map_err(iceberg::Error::from)
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        let result: Result<Namespace> = async {
            let root = Self::load_root_for_read(&self.storage).await?;
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
            let root = Self::load_root_for_read(&self.storage).await?;
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
        let namespace_owned = namespace.clone();
        let properties_owned = properties;
        let result: Result<()> = self
            .update_root(move |root| {
                if !root.update_namespace(&namespace_owned, properties_owned.clone()) {
                    return Err(Error::NamespaceNotFound(namespace_owned.clone()));
                }
                Ok(())
            })
            .await;

        result.map_err(iceberg::Error::from)
    }

    /// Drop an existing namespace.
    ///
    /// The namespace must be empty: no tables and no descendant namespaces.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        let namespace_owned = namespace.clone();
        let result: Result<()> = self
            .update_root(move |root| {
                root.drop_namespace(&namespace_owned)?;
                Ok(())
            })
            .await;

        result.map_err(iceberg::Error::from)
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        let result: Result<Vec<TableIdent>> = async {
            let root = Self::load_root_for_read(&self.storage).await?;
            Ok(root.active_tables_in_namespace(namespace))
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    /// Create and register new table
    async fn create_table(&self, namespace: &NamespaceIdent, creation: TableCreation) -> IcebergResult<Table> {
        let result: Result<Table> = async {
            // Build metadata ONCE outside the retry loop. The UUID-suffixed
            // path is reused across retries so a CAS conflict on the root does
            // not cascade orphans on every attempt.
            let prepared = TableUpdate::create(namespace, creation, &self.tables_uri_prefix)?;

            let root = Self::load_root_for_read(&self.storage).await?;
            root.validate_new_table(&prepared)?;
            drop(root);

            self.storage
                .write_table_metadata(
                    prepared.updated.metadata_location().as_str(),
                    prepared.updated.metadata(),
                )
                .await?;

            let prepared_arc = Arc::new(prepared);
            let prepared_for_op = Arc::clone(&prepared_arc);

            // Build the table handle *before* the CAS. Its identity is fully
            // fixed by the already prepared metadata, so building first keeps every
            // fallible step ahead of the durable publish: once `update_root`
            // lands the link, nothing left can fail and turn a committed root
            // into an `Err`.
            let table = Table::builder()
                .identifier(prepared_arc.full_name.clone())
                .metadata_location(prepared_arc.updated.metadata_location().as_str().to_string())
                .metadata(prepared_arc.updated.metadata().clone())
                .file_io(self.file_io.clone())
                .build()
                .map_err(Error::from)?;

            // On a root CAS conflict the op simply replays against the freshly
            // loaded root: `create_table` links the table when the name is free
            // and converges to success when our own prior write already landed.
            self.update_root(move |root| {
                root.create_table(&prepared_for_op)?;
                Ok(())
            })
            .await?;

            Ok(table)
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let result: Result<Table> = async {
            let root = Self::load_root_for_read(&self.storage).await?;
            let key = TableKey::from_ident(table);
            let entry = root.get_active(&key).ok_or_else(|| Error::TableNotFound(table.clone()))?;
            let metadata = self.storage.read_table_metadata(entry.metadata_location().as_str()).await?;

            Table::builder()
                .identifier(table.clone())
                .metadata_location(entry.metadata_location().as_str().to_string())
                .metadata(Arc::unwrap_or_clone(metadata))
                .file_io(self.file_io.clone())
                .build()
                .map_err(Error::from)
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let key = TableKey::from_ident(table);
        let table_ident = table.clone();
        let result: Result<()> = async {
            let root = Self::load_root_for_read(&self.storage).await?;
            let table_id = *root
                .get_active(&key)
                .ok_or_else(|| Error::TableNotFound(table_ident.clone()))?
                .table_id();

            self.update_root(move |root| {
                root.tombstone(&key, &table_id)?;
                Ok(())
            })
            .await
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let result: Result<bool> = async {
            let root = Self::load_root_for_read(&self.storage).await?;
            Ok(root.get_active(&TableKey::from_ident(table)).is_some())
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        let src_key = TableKey::from_ident(src);
        let dst_key = TableKey::from_ident(dest);
        let src_ident = src.clone();
        let result: Result<()> = async {
            let root = Self::load_root_for_read(&self.storage).await?;
            let table_id = *root
                .get_active(&src_key)
                .ok_or_else(|| Error::TableNotFound(src_ident.clone()))?
                .table_id();

            self.update_root(move |root| {
                root.rename(&src_key, dst_key.clone(), &table_id)?;
                Ok(())
            })
            .await
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    /// Register table - link existing table to Catalog
    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> IcebergResult<Table> {
        let result: Result<Table> = async {
            let metadata = self.storage.read_table_metadata(&metadata_location).await?;
            let table_id = TableId::from(metadata.uuid());
            let key = TableKey::from_ident(table);
            let typed_metadata_location = TableMetadataLocation::new(metadata_location.clone());
            let entry = CatalogTableLink::new(table_id, typed_metadata_location.clone());

            // Build the table handle *before* the CAS: its identity is fixed by
            // the metadata just read, so nothing fallible remains once
            // `update_root` publishes the link durably and the success path
            // cannot turn a committed root into an `Err`.
            let registered = Table::builder()
                .identifier(table.clone())
                .metadata_location(metadata_location.clone())
                .metadata(Arc::unwrap_or_clone(metadata))
                .file_io(self.file_io.clone())
                .build()
                .map_err(Error::from)?;

            let key_for_op = key;
            let entry_for_op = entry;
            let table_id_for_op = table_id;
            let metadata_location_for_op = typed_metadata_location;

            self.update_root(move |root| {
                // Lost-response convergence mirrors `create_table`: an entry
                // under our key with the same table_id and metadata_location is
                // our own prior register (success). Anything else falls through
                // to `link_table`, which rejects a foreign occupant as
                // `TableAlreadyExists`.
                if let Some(existing) = root.get_active(&key_for_op) {
                    if existing.table_id() == &table_id_for_op
                        && existing.metadata_location() == &metadata_location_for_op
                    {
                        return Ok(());
                    }
                }
                root.link_table(key_for_op.clone(), entry_for_op.clone())?;
                Ok(())
            })
            .await?;

            Ok(registered)
        }
        .await;

        result.map_err(iceberg::Error::from)
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        // A single-request commit yields exactly one response. Surface a missing
        // response as an error instead of panicking — the contract is internal,
        // but `Result` keeps the no-panic guarantee of the public API.
        self.commit_transaction(vec![commit])
            .await
            .map_err(IcebergError::from)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                IcebergError::new(
                    ErrorKind::Unexpected,
                    "commit_transaction with one request must return one response",
                )
            })
    }
}
