//! Test-only doubles and bootstrap shared by the crate's inline tests.
//!
//! Compiled only under `cfg(test)`; nothing here is part of the crate's API.

use std::collections::HashMap;
use std::future::pending;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation, TableIdent};
use tokio::net::TcpListener;

use crate::engine::{QueryEngine, QueryEngineConfig};

/// Catalog whose every call never completes.
///
/// Models the failure the query deadline exists for — a catalog that accepted
/// the request and then went silent — without a sleep whose duration the test
/// would have to guess.
#[derive(Debug)]
pub struct StallingCatalog;

#[async_trait]
impl Catalog for StallingCatalog {
    async fn list_namespaces(&self, _parent: Option<&NamespaceIdent>) -> IcebergResult<Vec<NamespaceIdent>> {
        pending().await
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        pending().await
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        pending().await
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> IcebergResult<bool> {
        pending().await
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        pending().await
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
        pending().await
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        pending().await
    }

    async fn create_table(&self, _namespace: &NamespaceIdent, _creation: TableCreation) -> IcebergResult<Table> {
        pending().await
    }

    async fn load_table(&self, _table: &TableIdent) -> IcebergResult<Table> {
        pending().await
    }

    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        pending().await
    }

    async fn purge_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        pending().await
    }

    async fn table_exists(&self, _table: &TableIdent) -> IcebergResult<bool> {
        pending().await
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        pending().await
    }

    async fn register_table(&self, _table: &TableIdent, _metadata_location: String) -> IcebergResult<Table> {
        pending().await
    }

    async fn update_table(&self, _commit: TableCommit) -> IcebergResult<Table> {
        pending().await
    }
}

/// Build an engine over [`StallingCatalog`] whose queries may run for
/// `max_query_duration_secs` before the transports cut them off.
pub fn build_stalling_engine(max_query_duration_secs: u64) -> Arc<QueryEngine> {
    let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let wal_reader =
        Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(&wal_store), 8192).expect("build WAL reader"));
    let config = QueryEngineConfig {
        max_query_duration_secs,
        ..QueryEngineConfig::default()
    };
    Arc::new(QueryEngine::new(
        Arc::new(StallingCatalog),
        config,
        wal_store,
        wal_reader,
    ))
}

/// Serve `router` on an OS-assigned local port and return its base URL together
/// with the server task, so a test drives it over real HTTP rather than through
/// `tower::ServiceExt::oneshot`.
pub async fn serve_router(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind test listener");
    let addr = listener.local_addr().expect("listener address");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("http://{addr}"), handle)
}
