//! Run command implementation

use std::{path::PathBuf, sync::Arc};

use futures::stream::{FuturesUnordered, StreamExt};
use icegate_common::{
    CatalogBuilder, IoHandle, ListenerRuntime, MemoryPressure, MetricsRuntime, run_operational_server,
    start_listener_runtime,
};
use icegate_queue::ParquetQueueReader;
use tokio_util::sync::CancellationToken;

use crate::{
    QueryConfig,
    engine::QueryEngine,
    error::QueryError,
    infra::{metrics::QueryMetrics, runtime::QueryRuntime},
};

/// Wait for shutdown signal (SIGINT or SIGTERM)
#[allow(clippy::expect_used)] // Signal handler registration failures are critical startup errors
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            tracing::info!("Received SIGINT (Ctrl+C)");
        }
        () = terminate => {
            tracing::info!("Received SIGTERM");
        }
    }
}

/// Execute the run command
///
/// Starts all enabled query servers and runs until Ctrl+C
pub async fn execute(config_path: PathBuf) -> Result<(), QueryError> {
    // Load configuration
    let config = QueryConfig::from_file(&config_path)?;

    // Initialize tracing with OpenTelemetry
    let tracing_guard = icegate_common::init_tracing(&config.tracing)?;

    tracing::info!("Loading configuration from {:?}", config_path);
    tracing::info!("Configuration loaded successfully");

    // Initialize metrics early so that the global meter provider is available
    // for OpenDAL's OtelMetricsLayer and Iceberg's IceGateStorageFactory.
    let metrics_runtime = if config.metrics.enabled {
        Some(Arc::new(MetricsRuntime::new("query")?))
    } else {
        None
    };

    // Cancellation token for coordinated shutdown. Created before the catalog
    // so the S3 catalog's CAS/transient retry loops abort promptly on SIGINT/
    // SIGTERM instead of running to their retry budget during shutdown.
    let cancel_token = CancellationToken::new();

    // Loaded before anything else starts, so a pair that is missing or does not match stops
    // the process here rather than once the catalog and the query engine are already
    // running. One runtime for every listener: they present the one certificate, and its
    // files are watched by one task.
    let listener_runtime = start_listener_runtime(config.tls.as_ref(), &cancel_token)?;

    // Process-wide memory-pressure guard shared by every server task. Inert
    // (never sheds) when no finite cgroup limit exists (dev/CI/bare-metal).
    let pressure = MemoryPressure::spawn(config.memory_pressure.clone(), cancel_token.clone());

    // Initialize catalog
    tracing::info!("Initializing catalog");
    let io_cache = IoHandle::from_config(config.catalog.cache.as_ref(), Some(&config.storage.backend)).await?;

    // Register foyer cache metrics with the meter provider so Prometheus can
    // scrape memory usage, disk I/O, entry counts, and throttle state.
    if let (Some(cache), Some(runtime)) = (io_cache.cache(), metrics_runtime.as_ref()) {
        icegate_common::register_foyer_metrics(cache, &runtime.meter());
    }

    let catalog = CatalogBuilder::from_config(&config.catalog, &io_cache, cancel_token.clone()).await?;

    tracing::info!("Catalog initialized successfully");

    // Validate engine config
    config.engine.validate()?;

    // Initialize the WAL object store from queue config. It comes from the same
    // IO handle as the catalog's storage, so the WAL shares the foyer cache,
    // the prefetch configuration, and the stat TTL with it.
    tracing::info!(queue_base_path = %config.queue.common.base_path, "Initializing WAL object store");
    let (store, prefix) = io_cache
        .object_store_operator_registry()
        .resolve_object_store(&config.queue.common.base_path)?;

    // Build the shared WAL queue reader with metadata caching
    let wal_reader = ParquetQueueReader::with_metadata_entries_cache_capacity(
        prefix,
        Arc::clone(&store),
        config.engine.batch_size,
        config.queue.read.metadata_entries_cache_capacity,
    )
    .map_err(|e| QueryError::Config(format!("Failed to create WAL reader: {e}")))?;

    // Initialize query engine with background catalog refresh.
    tracing::info!("Initializing query engine");
    let query_engine = Arc::new(QueryEngine::new(
        catalog,
        config.engine.clone(),
        store,
        Arc::new(wal_reader),
    ));
    query_engine.start_background_refresh();

    tracing::info!("Query engine initialized successfully");

    // Build query-level metrics instruments from the (already-initialized) meter.
    let query_metrics = Arc::new(
        metrics_runtime.as_ref().map_or_else(QueryMetrics::new_disabled, |runtime| {
            QueryMetrics::new(&runtime.meter())
        }),
    );

    // The tenant policy, resolved once. The line is the only place an operator
    // learns which mode the deployment runs in before a request is refused;
    // ingest prints the same one.
    let tenant_resolver = config.tenant.clone().into_resolver()?;
    tracing::info!(mode = tenant_resolver.mode(), "Tenant policy resolved");

    let runtime = QueryRuntime {
        engine: Arc::clone(&query_engine),
        metrics: Arc::clone(&query_metrics),
        pressure,
        tenant_resolver,
    };

    // Spawn one task per enabled server (metrics + each query API).
    let handles = spawn_servers(&config, &runtime, metrics_runtime.as_ref(), &listener_runtime);

    tracing::info!("All enabled query servers started");
    tracing::info!("Press Ctrl+C or send SIGTERM to shutdown");
    let failure = await_shutdown(handles, &cancel_token).await;

    if let Some(err) = failure {
        io_cache.close().await;
        drop(tracing_guard);
        return Err(err);
    }

    tracing::info!("All query servers stopped gracefully");

    // Gracefully close the IO cache to drain foyer's background flusher tasks.
    // This prevents "sending on a closed channel" errors during runtime teardown.
    io_cache.close().await;

    // Keep tracing guard alive until the very end
    drop(tracing_guard);

    Ok(())
}

/// Join handle for a spawned server task.
///
/// Every server (metrics + each query API) is normalised to this single
/// output type so [`await_shutdown`] can drain them through one
/// `FuturesUnordered`.
type ServerHandle = tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

/// Spawn one async task per enabled server, returning their join handles.
///
/// The metrics server's error is boxed to match the query servers' return
/// type so the caller can treat every handle uniformly. Never returns an empty
/// vector: the operational listener is spawned whatever the configuration says,
/// and [`QueryConfig::validate`] has already refused a document enabling no
/// query protocol.
///
/// Every server binds with a clone of `listener_runtime`, so they stop on its
/// token and, when it carries a certificate, all present that one.
fn spawn_servers(
    config: &QueryConfig,
    runtime: &QueryRuntime,
    metrics_runtime: Option<&Arc<MetricsRuntime>>,
    listener_runtime: &ListenerRuntime,
) -> Vec<ServerHandle> {
    let mut handles = Vec::new();

    // Operational server. Spawned whatever `metrics.enabled` says: the listener
    // owns `/health`, and the kubelet probes it even on a deployment that
    // scrapes no metrics.
    {
        let metrics_config = config.metrics.clone();
        let listener_runtime = listener_runtime.clone();
        let registry = metrics_runtime.map(|runtime| runtime.registry());
        handles.push(tokio::spawn(async move {
            run_operational_server(metrics_config, registry, listener_runtime)
                .await
                .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
        }));
    }

    // Query servers. Each takes a clone of the one runtime: four refcount
    // bumps per server, taken here and never again.
    if config.loki.enabled {
        let runtime = runtime.clone();
        let loki_config = config.loki.clone();
        let listener_runtime = listener_runtime.clone();
        handles.push(tokio::spawn(async move {
            crate::loki::run(runtime, loki_config, listener_runtime).await
        }));
    }

    if config.prometheus.enabled {
        let runtime = runtime.clone();
        let prom_config = config.prometheus.clone();
        let listener_runtime = listener_runtime.clone();
        handles.push(tokio::spawn(async move {
            crate::prometheus::run(runtime, prom_config, listener_runtime).await
        }));
    }

    if config.tempo.enabled {
        let runtime = runtime.clone();
        let tempo_config = config.tempo.clone();
        let listener_runtime = listener_runtime.clone();
        handles.push(tokio::spawn(async move {
            crate::tempo::run(runtime, tempo_config, listener_runtime).await
        }));
    }

    if config.flight_sql.enabled {
        let runtime = runtime.clone();
        let flight_sql_config = config.flight_sql.clone();
        let listener_runtime = listener_runtime.clone();
        handles.push(tokio::spawn(async move {
            crate::flight_sql::run(runtime, flight_sql_config, listener_runtime).await
        }));
    }

    handles
}

/// Drive all server tasks until shutdown, returning the first failure.
///
/// Waits for a shutdown signal (SIGINT/SIGTERM) or for any task to exit,
/// then cancels the shared token and drains the rest. A task that exits
/// before the signal — or returns an error, or fails to join — is recorded
/// as the failure to propagate.
async fn await_shutdown(handles: Vec<ServerHandle>, cancel_token: &CancellationToken) -> Option<QueryError> {
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    let mut handles: FuturesUnordered<_> = handles.into_iter().collect();
    let mut shutdown_started = false;
    let mut failure = None;

    while !handles.is_empty() {
        tokio::select! {
            () = &mut shutdown, if !shutdown_started => {
                tracing::info!("Shutdown signal received, stopping all servers...");
                cancel_token.cancel();
                shutdown_started = true;
            }
            result = handles.next() => {
                let Some(result) = result else {
                    continue;
                };

                match result {
                    Ok(Ok(())) if shutdown_started => {}
                    Ok(Ok(())) => {
                        tracing::error!("Server task exited before shutdown signal");
                        failure.get_or_insert_with(|| {
                            QueryError::Internal("server task exited before shutdown signal".to_string())
                        });
                    }
                    Ok(Err(err)) => {
                        tracing::error!("Server task failed: {err}");
                        failure.get_or_insert_with(|| QueryError::Internal(err.to_string()));
                    }
                    Err(err) => {
                        tracing::error!("Server task join failed: {err}");
                        failure.get_or_insert_with(|| QueryError::Internal(format!("server task join failed: {err}")));
                    }
                }

                if failure.is_some() && !shutdown_started {
                    tracing::info!("Stopping remaining servers after early task exit...");
                    cancel_token.cancel();
                    shutdown_started = true;
                }
            }
        }
    }

    failure
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        net::{Ipv4Addr, SocketAddr},
        time::Duration,
    };

    use icegate_common::{
        DEFAULT_TLS_RELOAD_INTERVAL_SECS, HEALTH_PATH, OperationalConfig, ServerCertificate, TenantResolver, TlsConfig,
    };
    use rcgen::{CertificateParams, KeyPair};
    use reqwest::{Client, StatusCode, Version};
    use tempfile::TempDir;
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

    use super::*;
    use crate::{
        flight_sql::FlightSqlConfig, loki::LokiConfig, prometheus::PrometheusConfig, tempo::TempoConfig,
        test_support::build_stalling_engine,
    };

    /// The upper bound of every wait, so a hang fails the test instead of the run.
    const STEP_TIMEOUT: Duration = Duration::from_secs(10);

    /// The name the test certificate is issued for, which the clients verify the server against.
    const CERTIFICATE_HOST: &str = "localhost";

    /// Ports nothing is listening on, all held at once so none of them is handed out twice.
    /// Another process may take one before the server binds, which the readiness wait reports
    /// as a server that never answered.
    fn reserve_ports<const COUNT: usize>() -> [u16; COUNT] {
        let reservations: Vec<_> = (0..COUNT)
            .map(|_| std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind a free port"))
            .collect();
        let mut ports = [0; COUNT];
        for (port, reservation) in ports.iter_mut().zip(&reservations) {
            *port = reservation.local_addr().expect("local address").port();
        }
        ports
    }

    /// Request `path` over `https` from the listener on `port` until it answers, with a client
    /// that trusts only `certificate_pem`, and return the answer's status and HTTP version.
    async fn get_over_tls(certificate_pem: &str, port: u16, path: &str) -> Result<(StatusCode, Version), String> {
        let root = reqwest::Certificate::from_pem(certificate_pem.as_bytes()).expect("parse the test certificate");
        let client = Client::builder()
            .tls_certs_only([root])
            .resolve(CERTIFICATE_HOST, SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
            .build()
            .expect("build the TLS client");
        let url = format!("https://{CERTIFICATE_HOST}:{port}{path}");
        let mut last_error = String::new();
        // The servers bind on their own tasks, so the first attempts may find the port closed.
        let attempts = async {
            loop {
                match client.get(&url).send().await {
                    Ok(response) => return (response.status(), response.version()),
                    Err(error) => last_error = error.to_string(),
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        let answer = tokio::time::timeout(STEP_TIMEOUT, attempts).await;
        answer.map_err(|_elapsed| format!("{url} never answered over TLS; last error: {last_error}"))
    }

    /// Every listener the binary spawns presents the one certificate it was handed, and speaks the
    /// protocol its clients do: `http/1.1` on the four HTTP listeners (the client offers `h2` as
    /// well), `h2` on Flight SQL (a gRPC channel opens only once it is negotiated). A listener
    /// left on plaintext would not complete the handshake, so each answer below proves its own
    /// listener.
    #[tokio::test]
    async fn every_spawned_listener_serves_tls_with_the_one_certificate() {
        let key_pair = KeyPair::generate().expect("generate key pair");
        let certificate = CertificateParams::new(vec![CERTIFICATE_HOST.to_string()])
            .expect("certificate params")
            .self_signed(&key_pair)
            .expect("self-sign certificate");
        let directory = TempDir::new().expect("temp directory");
        let certificate_path = directory.path().join("tls.crt");
        let private_key_path = directory.path().join("tls.key");
        fs::write(&certificate_path, certificate.pem()).expect("write certificate");
        fs::write(&private_key_path, key_pair.serialize_pem()).expect("write private key");
        let server_certificate = ServerCertificate::load(&TlsConfig {
            certificate_path,
            private_key_path,
            reload_interval_secs: DEFAULT_TLS_RELOAD_INTERVAL_SECS,
        })
        .expect("load the generated pair");

        let [
            loki_port,
            prometheus_port,
            tempo_port,
            flight_sql_port,
            operational_port,
        ] = reserve_ports();
        let host = Ipv4Addr::LOCALHOST.to_string();
        let config = QueryConfig {
            loki: LokiConfig {
                enabled: true,
                host: host.clone(),
                port: loki_port,
            },
            prometheus: PrometheusConfig {
                enabled: true,
                host: host.clone(),
                port: prometheus_port,
            },
            tempo: TempoConfig {
                enabled: true,
                host: host.clone(),
                port: tempo_port,
            },
            flight_sql: FlightSqlConfig {
                enabled: true,
                host: host.clone(),
                port: flight_sql_port,
                ..FlightSqlConfig::default()
            },
            metrics: OperationalConfig {
                host,
                port: operational_port,
                ..OperationalConfig::default()
            },
            ..QueryConfig::default()
        };
        let runtime = QueryRuntime {
            engine: build_stalling_engine(1),
            metrics: Arc::new(QueryMetrics::new_disabled()),
            pressure: MemoryPressure::inert(),
            tenant_resolver: TenantResolver::Multi,
        };
        let cancel_token = CancellationToken::new();
        let listener_runtime = ListenerRuntime::new(cancel_token.clone(), Some(Arc::new(server_certificate)));

        let handles = spawn_servers(&config, &runtime, None, &listener_runtime);

        // Four query servers and the operational one: the fixture reaches every listener.
        assert_eq!(handles.len(), 5);
        let pem = certificate.pem();
        let mut answers = Vec::new();
        for (name, port, path) in [
            ("Loki", loki_port, "/ready"),
            ("Prometheus", prometheus_port, "/-/ready"),
            ("Tempo", tempo_port, "/ready"),
            ("operational", operational_port, HEALTH_PATH),
        ] {
            answers.push((name, get_over_tls(&pem, port, path).await));
        }
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&pem))
            .domain_name(CERTIFICATE_HOST);
        // Dropped on the spot: an open channel would keep its connection open through the drain.
        let flight_sql_connected = tokio::time::timeout(
            STEP_TIMEOUT,
            Endpoint::from_shared(format!("https://{}:{flight_sql_port}", Ipv4Addr::LOCALHOST))
                .expect("endpoint")
                .tls_config(tls_config)
                .expect("client TLS config")
                .connect(),
        )
        .await
        .map(|connected| connected.map(drop));

        cancel_token.cancel();
        let mut outcomes = Vec::new();
        for handle in handles {
            outcomes.push(tokio::time::timeout(STEP_TIMEOUT, handle).await);
        }

        for (name, answer) in answers {
            assert_eq!(answer, Ok((StatusCode::OK, Version::HTTP_11)), "{name}");
        }
        assert!(
            matches!(flight_sql_connected, Ok(Ok(()))),
            "got {flight_sql_connected:?}"
        );
        for outcome in outcomes {
            assert!(matches!(outcome, Ok(Ok(Ok(())))), "got {outcome:?}");
        }
    }
}
