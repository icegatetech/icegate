//! The `OpenSearch` listener over a temporary warehouse, and a reqwest client.
//!
//! The catalog holds every IceGate table with its production schema, partition spec and sort
//! order, so per-tenant figures see the identity partition of `tenant_id`; the WAL store is in
//! memory and WAL reads are on. The listener binds port 0 under the `multi` tenant policy. Tests
//! send literal requests through [`TestServer::request`] and state their expected answers.
//! [`TestServer::start_replica`] adds a listener of the same deployment.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use iceberg::spec::{PartitionSpec, Schema, SortOrder};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use icegate_common::testing::server_task::{DrainOutcome, PORT_BIND_TIMEOUT, SHUTDOWN_TIMEOUT, drain_server_task};
use icegate_common::{
    CatalogBackend, CatalogBuilder, CatalogConfig, EVENTS_TABLE, ICEGATE_NAMESPACE, IoHandle, LOGS_TABLE,
    ListenerRuntime, METRICS_TABLE, MemoryPressure, OPERATIONS_TABLE, PRICES_TABLE, SPANS_TABLE, TENANT_ID_HEADER,
    TenantResolver, schema,
};
use icegate_query::engine::{QueryEngine, QueryEngineConfig};
use icegate_query::infra::metrics::QueryMetrics;
use icegate_query::infra::runtime::QueryRuntime;
use icegate_query::opensearch::{OpenSearchConfig, SystemObjectStore};
use object_store::memory::InMemory;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Why the runtime or the listener of a test could not be set up.
type SetupError = Box<dyn std::error::Error + Send + Sync>;

/// One production table: its name and the schema, partition spec and sort order builders of
/// `icegate_common::schema`.
type ProductionTable = (
    &'static str,
    fn() -> icegate_common::Result<Schema>,
    fn(&Schema) -> icegate_common::Result<PartitionSpec>,
    fn(&Schema) -> icegate_common::Result<SortOrder>,
);

/// Every table of the IceGate namespace.
const PRODUCTION_TABLES: [ProductionTable; 6] = [
    (
        LOGS_TABLE,
        schema::logs_schema,
        schema::logs_partition_spec,
        schema::logs_sort_order,
    ),
    (
        SPANS_TABLE,
        schema::spans_schema,
        schema::spans_partition_spec,
        schema::spans_sort_order,
    ),
    (
        EVENTS_TABLE,
        schema::events_schema,
        schema::events_partition_spec,
        schema::events_sort_order,
    ),
    (
        METRICS_TABLE,
        schema::metrics_schema,
        schema::metrics_partition_spec,
        schema::metrics_sort_order,
    ),
    (
        OPERATIONS_TABLE,
        schema::operations_schema,
        schema::operations_partition_spec,
        schema::operations_sort_order,
    ),
    (
        PRICES_TABLE,
        schema::prices_schema,
        schema::prices_partition_spec,
        schema::prices_sort_order,
    ),
];

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
    /// Held for its `Drop`, which removes the warehouse directory once no replica uses it.
    _warehouse: tempfile::TempDir,
}

/// A running `OpenSearch` listener over a temporary warehouse.
pub struct TestServer {
    /// Client for the listener.
    pub client: reqwest::Client,
    /// `http://127.0.0.1:<port>` of the listener.
    pub base_url: String,
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
        match Self::try_start_with_config(config).await {
            Ok(started) => started,
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
        match Self::try_start_with_system_store(config, system_object_store).await {
            Ok(started) => started,
            Err(error) => panic!("the OpenSearch listener did not start: {error}"),
        }
    }

    /// [`TestServer::start_with_config`] with its failure returned.
    async fn try_start_with_config(config: OpenSearchConfig) -> Result<(Self, Arc<dyn Catalog>), SetupError> {
        let system_object_store = build_in_memory_system_object_store()?;
        Self::try_start_with_system_store(config, system_object_store).await
    }

    /// Start another replica of this server's deployment: the same catalog, WAL store,
    /// configuration and system object store, with its own engine, caches and write loops. A
    /// replica that does not start fails the test here, with the step that failed.
    pub async fn start_replica(&self) -> Self {
        match Self::try_launch(Arc::clone(&self.deployment)).await {
            Ok(replica) => replica,
            Err(error) => panic!("the OpenSearch replica did not start: {error}"),
        }
    }

    /// [`TestServer::start_with_system_store`] with its failure returned.
    async fn try_start_with_system_store(
        config: OpenSearchConfig,
        system_object_store: SystemObjectStore,
    ) -> Result<(Self, Arc<dyn Catalog>), SetupError> {
        let (catalog, warehouse) = build_warehouse_catalog().await?;
        let deployment = Arc::new(Deployment {
            catalog: Arc::clone(&catalog),
            wal_store: Arc::new(InMemory::new()),
            config,
            system_store: system_object_store,
            _warehouse: warehouse,
        });
        Ok((Self::try_launch(deployment).await?, catalog))
    }

    /// Start one listener of `deployment` on a free port and wait until it answers. A listener
    /// already spawned when a later step fails is stopped before the error returns. One whose
    /// startup checks fail reports no port, and the drain re-raises its own failure, which names
    /// the check.
    async fn try_launch(deployment: Arc<Deployment>) -> Result<Self, SetupError> {
        let runtime = build_replica_runtime(&deployment.catalog, &deployment.wal_store)?;
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

/// The runtime a listener serves: a catalog over a temporary warehouse holding every table of
/// [`PRODUCTION_TABLES`], an in-memory WAL store with WAL reads on, and the `multi` tenant
/// policy. The returned directory removes the warehouse on drop, so the caller holds it for as
/// long as the runtime is used.
pub async fn build_runtime() -> Result<(QueryRuntime, Arc<dyn Catalog>, tempfile::TempDir), SetupError> {
    let (catalog, warehouse) = build_warehouse_catalog().await?;
    let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let runtime = build_replica_runtime(&catalog, &wal_store)?;
    Ok((runtime, catalog, warehouse))
}

/// A catalog over a temporary warehouse holding every table of [`PRODUCTION_TABLES`]. The returned
/// directory removes the warehouse on drop.
async fn build_warehouse_catalog() -> Result<(Arc<dyn Catalog>, tempfile::TempDir), SetupError> {
    let warehouse = tempfile::tempdir()?;
    let catalog_config = CatalogConfig {
        backend: CatalogBackend::Memory,
        warehouse: warehouse.path().to_str().ok_or("the warehouse path is not UTF-8")?.to_string(),
        properties: HashMap::default(),
        cache: None,
    };
    let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new()).await?;
    create_production_tables(&catalog).await?;
    Ok((catalog, warehouse))
}

/// The runtime of one replica over `catalog` and `wal_store`: its own engine with WAL reads on,
/// and the `multi` tenant policy.
fn build_replica_runtime(
    catalog: &Arc<dyn Catalog>,
    wal_store: &Arc<dyn object_store::ObjectStore>,
) -> Result<QueryRuntime, SetupError> {
    let wal_reader = Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(wal_store), 8192)?);
    let engine_config = QueryEngineConfig {
        wal_query_enabled: true,
        ..QueryEngineConfig::default()
    };
    Ok(QueryRuntime {
        engine: Arc::new(QueryEngine::new(
            Arc::clone(catalog),
            engine_config,
            Arc::clone(wal_store),
            wal_reader,
        )),
        metrics: Arc::new(QueryMetrics::new_disabled()),
        pressure: MemoryPressure::inert(),
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

/// Create every table of [`PRODUCTION_TABLES`] in the IceGate namespace.
async fn create_production_tables(catalog: &Arc<dyn Catalog>) -> Result<(), SetupError> {
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    for (name, build_schema, build_partition_spec, build_sort_order) in PRODUCTION_TABLES {
        let table_schema = build_schema()?;
        let creation = TableCreation::builder()
            .name(name.to_string())
            .partition_spec(build_partition_spec(&table_schema)?)
            .sort_order(build_sort_order(&table_schema)?)
            .schema(table_schema)
            .build();
        catalog.create_table(&namespace, creation).await?;
    }
    Ok(())
}
