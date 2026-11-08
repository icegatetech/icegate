//! The `OpenSearch` listener over a temporary warehouse, and a reqwest client.
//!
//! The catalog holds every IceGate table with its production schema, partition spec and sort
//! order, so per-tenant figures see the identity partition of `tenant_id`; the WAL store is in
//! memory and WAL reads are on. The listener binds port 0 under the `multi` tenant policy. Tests
//! send literal requests through [`TestServer::request`] and state their expected answers.
//! [`TestServer::start_replica`] adds a listener of the same deployment. [`ServerOptions`] drive
//! one failure path of every replica a deployment starts.

use std::collections::HashMap;
use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation, TableIdent};
use icegate_common::testing::server_task::{DrainOutcome, PORT_BIND_TIMEOUT, SHUTDOWN_TIMEOUT, drain_server_task};
use icegate_common::{
    CatalogBackend, CatalogBuilder, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, ListenerRuntime, MemoryPressure,
    TENANT_ID_HEADER, TenantResolver,
};
use icegate_query::engine::{QueryEngine, QueryEngineConfig};
use icegate_query::infra::metrics::QueryMetrics;
use icegate_query::infra::runtime::QueryRuntime;
use icegate_query::opensearch::{OpenSearchConfig, SystemObjectStore};
use object_store::memory::InMemory;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::test_rows::create_production_tables;

/// Why the runtime or the listener of a test could not be set up.
type SetupError = Box<dyn std::error::Error + Send + Sync>;

/// Readiness poll budget for a freshly bound listener: generous, because the sanitizer builds
/// run this harness at a fraction of normal speed.
const READY_ATTEMPTS: u32 = 50;
/// Pause between two readiness polls.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Bound on one readiness probe, so a listener that accepts a connection and never answers
/// still lets the poll budget run out.
const READY_PROBE_TIMEOUT: Duration = Duration::from_millis(200);
/// Bound on every request a test sends, so a listener that stops answering fails the test
/// instead of hanging it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Location of the in-memory system-index store a harness listener uses unless the test passes
/// its own store.
const IN_MEMORY_SYSTEM_LOCATION: &str = "s3://opensearch-test/opensearch-system";

/// Server knobs a test sets to drive one failure path. Every replica of a deployment starts with
/// the same options.
#[derive(Clone)]
pub struct ServerOptions {
    /// The memory-pressure handle the shedding layer reads.
    pub pressure: MemoryPressure,
    /// `engine.max_query_duration_secs`, the deadline of every request.
    pub max_query_duration_secs: u64,
    /// When set, the engine reads its catalog through a [`StallSwitchCatalog`] that stops
    /// answering once the test sets the switch, after the server has started.
    pub catalog_stall_switch: Option<Arc<AtomicBool>>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            pressure: MemoryPressure::inert(),
            max_query_duration_secs: QueryEngineConfig::default().max_query_duration_secs,
            catalog_stall_switch: None,
        }
    }
}

/// A catalog that answers like `inner` until `is_stalled` is set, and never answers after.
///
/// Startup reads the catalog (the system-index location check lists every table), so a catalog
/// that never answers from the start would keep the server from binding. The switch lets the
/// server start and then models the failure the deadline exists for: a catalog that went
/// silent.
#[derive(Debug)]
pub struct StallSwitchCatalog {
    /// The catalog answering while the switch is off.
    inner: Arc<dyn Catalog>,
    /// Set by the test once the server has started.
    is_stalled: Arc<AtomicBool>,
}

impl StallSwitchCatalog {
    /// Return at once while the switch is off; never return once it is on.
    async fn wait_unless_stalled(&self) {
        if self.is_stalled.load(Ordering::SeqCst) {
            pending::<()>().await;
        }
    }
}

#[async_trait]
impl Catalog for StallSwitchCatalog {
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>) -> IcebergResult<Vec<NamespaceIdent>> {
        self.wait_unless_stalled().await;
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.wait_unless_stalled().await;
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.wait_unless_stalled().await;
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.wait_unless_stalled().await;
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.wait_unless_stalled().await;
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.wait_unless_stalled().await;
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.wait_unless_stalled().await;
        self.inner.list_tables(namespace).await
    }

    async fn create_table(&self, namespace: &NamespaceIdent, creation: TableCreation) -> IcebergResult<Table> {
        self.wait_unless_stalled().await;
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.wait_unless_stalled().await;
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.wait_unless_stalled().await;
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.wait_unless_stalled().await;
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.wait_unless_stalled().await;
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, source: &TableIdent, destination: &TableIdent) -> IcebergResult<()> {
        self.wait_unless_stalled().await;
        self.inner.rename_table(source, destination).await
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> IcebergResult<Table> {
        self.wait_unless_stalled().await;
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.wait_unless_stalled().await;
        self.inner.update_table(commit).await
    }
}

/// What the replicas of one deployment share.
struct Deployment {
    /// The catalog over the temporary warehouse.
    catalog: Arc<dyn Catalog>,
    /// The WAL store.
    wal_store: Arc<dyn object_store::ObjectStore>,
    /// The configuration every replica starts from, before its host and port are overridden.
    config: OpenSearchConfig,
    /// The object store holding the system indices.
    system_store: SystemObjectStore,
    /// The server knobs every replica of the deployment starts with.
    options: ServerOptions,
    /// Held for its `Drop`, which removes the warehouse directory once no replica uses it.
    _warehouse: tempfile::TempDir,
}

/// A running `OpenSearch` listener over a temporary warehouse.
pub struct TestServer {
    /// Client for the listener.
    pub client: reqwest::Client,
    /// `http://127.0.0.1:<port>` of the listener.
    pub base_url: String,
    /// The WAL store the engine reads hot segments from; tests write segments into it.
    pub wal_store: Arc<dyn object_store::ObjectStore>,
    /// Cancels the listener: [`TestServer::shutdown`] and the `Drop` fire it.
    cancel_token: CancellationToken,
    /// The task serving the listener, drained on shutdown and aborted on drop.
    server_handle: tokio::task::JoinHandle<()>,
    /// Holding it keeps the shared deployment, and with it the warehouse, alive.
    deployment: Arc<Deployment>,
}

impl TestServer {
    /// Start with the default configuration.
    pub async fn start() -> (Self, Arc<dyn Catalog>) {
        Self::start_with_config(OpenSearchConfig::default()).await
    }

    /// Start with `config`, whose `enabled`, `host` and `port` are overridden for the test, and an
    /// in-memory system-index store. A listener that does not start fails the test here, with the
    /// step that failed.
    pub async fn start_with_config(config: OpenSearchConfig) -> (Self, Arc<dyn Catalog>) {
        Self::start_with_options(config, ServerOptions::default()).await
    }

    /// Start like [`TestServer::start_with_config`], with `options` driving one failure path. The
    /// warehouse catalog is returned to the test, which writes rows through it even when the
    /// engine reads through a stall switch.
    pub async fn start_with_options(config: OpenSearchConfig, options: ServerOptions) -> (Self, Arc<dyn Catalog>) {
        let start_result = match build_in_memory_system_object_store() {
            Ok(system_object_store) => Self::try_start_deployment(config, system_object_store, options).await,
            Err(error) => Err(error),
        };
        match start_result {
            Ok(started_deployment) => started_deployment,
            Err(error) => panic!("the OpenSearch listener did not start: {error}"),
        }
    }

    /// Start with `config`, whose `enabled`, `host` and `port` are overridden for the test, and
    /// with the system indices in `system_object_store`. A listener that does not start fails the
    /// test here, with the step that failed.
    pub async fn start_with_system_store(
        config: OpenSearchConfig,
        system_object_store: SystemObjectStore,
    ) -> (Self, Arc<dyn Catalog>) {
        match Self::try_start_deployment(config, system_object_store, ServerOptions::default()).await {
            Ok(started) => started,
            Err(error) => panic!("the OpenSearch listener did not start: {error}"),
        }
    }

    /// Start another replica of this server's deployment: the same catalog, WAL store,
    /// configuration, system object store and options, with its own engine, caches and write
    /// loops. A replica that does not start fails the test here, with the step that failed.
    pub async fn start_replica(&self) -> Self {
        match Self::try_launch(Arc::clone(&self.deployment)).await {
            Ok(replica) => replica,
            Err(error) => panic!("the OpenSearch replica did not start: {error}"),
        }
    }

    /// A new deployment with its own warehouse, its production tables created, and its first
    /// replica started, with its failure returned.
    async fn try_start_deployment(
        config: OpenSearchConfig,
        system_object_store: SystemObjectStore,
        options: ServerOptions,
    ) -> Result<(Self, Arc<dyn Catalog>), SetupError> {
        let (catalog, warehouse) = build_warehouse_catalog().await?;
        let deployment = Arc::new(Deployment {
            catalog: Arc::clone(&catalog),
            wal_store: Arc::new(InMemory::new()),
            config,
            system_store: system_object_store,
            options,
            _warehouse: warehouse,
        });
        Ok((Self::try_launch(deployment).await?, catalog))
    }

    /// Start one listener of `deployment` on a free port and wait until it answers. A listener
    /// already spawned when a later step fails is stopped before the error returns. One whose
    /// startup checks fail reports no port, and the drain re-raises its own failure, which names
    /// the check.
    async fn try_launch(deployment: Arc<Deployment>) -> Result<Self, SetupError> {
        let runtime = build_replica_runtime(&deployment.catalog, &deployment.wal_store, &deployment.options)?;
        // Built before the listener is spawned, so its failure leaves no task running.
        let client = reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build()?;
        // Port 0: the OS assigns a free port and the listener reports it, so no probe races
        // another process for it.
        let config = OpenSearchConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 0,
            ..deployment.config.clone()
        };
        let system_object_store = deployment.system_store.clone();
        let cancel_token = CancellationToken::new();
        let server_token = cancel_token.clone();
        let (port_tx, port_rx) = oneshot::channel::<u16>();
        let mut server_handle = tokio::spawn(async move {
            let listener_runtime = ListenerRuntime::plaintext(server_token);
            let outcome = icegate_query::opensearch::run_with_port_tx(
                runtime,
                config,
                system_object_store,
                listener_runtime,
                Some(port_tx),
            )
            .await;
            // The task's panic is how a listener failure reaches the test: the drain re-raises it.
            if let Err(error) = outcome {
                panic!("the OpenSearch listener failed: {error}");
            }
        });
        let Ok(Ok(port)) = tokio::time::timeout(PORT_BIND_TIMEOUT, port_rx).await else {
            cancel_token.cancel();
            drain_server_task(&mut server_handle, "OpenSearch").await;
            return Err("it reported no bound port".into());
        };
        let base_url = format!("http://127.0.0.1:{port}");
        if !is_serving(&client, &base_url).await {
            cancel_token.cancel();
            drain_server_task(&mut server_handle, "OpenSearch").await;
            return Err(format!("it bound {base_url} but never answered").into());
        }
        Ok(Self {
            client,
            base_url,
            wal_store: Arc::clone(&deployment.wal_store),
            cancel_token,
            server_handle,
            deployment,
        })
    }

    /// A request to `path_and_query` carrying `tenant` in `x-scope-orgid`, or no tenant header.
    pub fn request(
        &self,
        method: reqwest::Method,
        tenant: Option<&str>,
        path_and_query: &str,
    ) -> reqwest::RequestBuilder {
        let mut builder = self.client.request(method, format!("{}{path_and_query}", self.base_url));
        if let Some(tenant) = tenant {
            builder = builder.header(TENANT_ID_HEADER, tenant);
        }
        builder
    }

    /// `POST` `body` to a PPL-family path as `tenant`; keeps the status, content type and text.
    pub async fn post_ppl(&self, tenant: &str, path_and_query: &str, body: &serde_json::Value) -> PplReply {
        let response = self
            .request(reqwest::Method::POST, Some(tenant), path_and_query)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap_or_else(|error| panic!("PPL request failed: {error}"));
        PplReply::from_response(response).await
    }

    /// Stop the listener. A panic or a hang of the server task fails the test.
    pub async fn shutdown(mut self) {
        self.cancel_token.cancel();
        let outcome = drain_server_task(&mut self.server_handle, "OpenSearch").await;
        assert_eq!(
            outcome,
            DrainOutcome::Finished,
            "the OpenSearch listener did not stop within {}s",
            SHUTDOWN_TIMEOUT.as_secs()
        );
    }
}

impl Drop for TestServer {
    /// A test that panics before [`TestServer::shutdown`] still stops its listener; a synchronous
    /// drop can only schedule the abort, which is enough to stop it serving.
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.server_handle.abort();
    }
}

/// The runtime a listener serves: a catalog over a temporary warehouse holding every table
/// [`create_production_tables`] creates, an in-memory WAL store with WAL reads on, and the
/// `multi` tenant policy. The returned directory removes the warehouse on drop,
/// so the caller holds it for as long as the runtime is used.
pub async fn build_runtime() -> Result<(QueryRuntime, Arc<dyn Catalog>, tempfile::TempDir), SetupError> {
    let (catalog, warehouse) = build_warehouse_catalog().await?;
    let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let runtime = build_replica_runtime(&catalog, &wal_store, &ServerOptions::default())?;
    Ok((runtime, catalog, warehouse))
}

/// A catalog over a temporary warehouse holding the IceGate namespace and every table
/// [`create_production_tables`] creates. The returned directory removes the warehouse on drop.
async fn build_warehouse_catalog() -> Result<(Arc<dyn Catalog>, tempfile::TempDir), SetupError> {
    let warehouse = tempfile::tempdir()?;
    let catalog_config = CatalogConfig {
        backend: CatalogBackend::Memory,
        warehouse: warehouse.path().to_str().ok_or("the warehouse path is not UTF-8")?.to_string(),
        properties: HashMap::default(),
        cache: None,
    };
    let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new()).await?;
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await?;
    create_production_tables(&catalog).await?;
    Ok((catalog, warehouse))
}

/// The runtime of one replica over `catalog` and `wal_store`: its own engine with WAL reads on,
/// the `multi` tenant policy, and the deadline, memory pressure and catalog stall switch of
/// `options`. With a stall switch the engine reads `catalog` through it, while the test keeps
/// writing rows through `catalog` itself.
fn build_replica_runtime(
    catalog: &Arc<dyn Catalog>,
    wal_store: &Arc<dyn object_store::ObjectStore>,
    options: &ServerOptions,
) -> Result<QueryRuntime, SetupError> {
    let wal_reader = Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(wal_store), 8192)?);
    let engine_config = QueryEngineConfig {
        wal_query_enabled: true,
        max_query_duration_secs: options.max_query_duration_secs,
        ..QueryEngineConfig::default()
    };
    let engine_catalog: Arc<dyn Catalog> = match &options.catalog_stall_switch {
        Some(switch) => Arc::new(StallSwitchCatalog {
            inner: Arc::clone(catalog),
            is_stalled: Arc::clone(switch),
        }),
        None => Arc::clone(catalog),
    };
    Ok(QueryRuntime {
        engine: Arc::new(QueryEngine::new(
            engine_catalog,
            engine_config,
            Arc::clone(wal_store),
            wal_reader,
        )),
        metrics: Arc::new(QueryMetrics::new_disabled()),
        pressure: options.pressure.clone(),
        tenant_resolver: TenantResolver::Multi,
    })
}

/// An in-memory system-index store that passes the listener's startup checks: `InMemory`
/// supports every conditional operation, and its location overlaps no table of the temporary
/// warehouse.
pub fn build_in_memory_system_object_store() -> Result<SystemObjectStore, SetupError> {
    Ok(SystemObjectStore::new(
        Arc::new(InMemory::new()),
        IN_MEMORY_SYSTEM_LOCATION,
    )?)
}

/// Whether the listener at `base_url` answers HTTP within the poll budget. Any answer counts:
/// without a tenant header it is the `400` refusal. Each probe is bounded on its own, so the
/// budget bounds the whole wait.
async fn is_serving(client: &reqwest::Client, base_url: &str) -> bool {
    for _ in 0..READY_ATTEMPTS {
        let probe = client.head(format!("{base_url}/")).timeout(READY_PROBE_TIMEOUT);
        if probe.send().await.is_ok() {
            return true;
        }
        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
    false
}

/// One answer of a PPL-family endpoint. The body stays text: PPL execution errors are `text/plain`,
/// every other answer is JSON.
pub struct PplReply {
    /// HTTP status.
    pub status: u16,
    /// The `content-type` header, empty when absent.
    pub content_type: String,
    /// The body as text.
    pub text: String,
}

impl PplReply {
    /// The answer `response` carries, for a PPL-family request sent by hand; panics when its body
    /// cannot be read.
    pub async fn from_response(response: reqwest::Response) -> Self {
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .map(|value| {
                value
                    .to_str()
                    .unwrap_or_else(|error| panic!("content type is not ASCII: {error}"))
                    .to_owned()
            })
            .unwrap_or_default();
        let text = response
            .text()
            .await
            .unwrap_or_else(|error| panic!("PPL response body: {error}"));
        Self {
            status,
            content_type,
            text,
        }
    }

    /// The body parsed as JSON; panics with the body when it is not JSON.
    pub fn parse_json_body(&self) -> serde_json::Value {
        serde_json::from_str(&self.text).unwrap_or_else(|error| panic!("body is not JSON ({error}): {}", self.text))
    }
}
