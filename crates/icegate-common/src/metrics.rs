//! Prometheus metrics, and the operational HTTP listener that serves them.
//!
//! The listener does two things that are not both about metrics: it always
//! answers [`HEALTH_PATH`], and it answers [`OperationalConfig::path`] only when
//! the component built a [`MetricsRuntime`]. It lives here, and its
//! configuration is named after the metrics endpoint, because that is the name
//! of the section every deployment configures it through — `metrics` in each
//! component's configuration file and in the chart's values. Renaming the module
//! would leave the key and the code that reads it named differently.
//!
//! So [`OperationalConfig::enabled`] governs the Prometheus endpoint alone: the
//! listener binds either way, and a liveness probe never depends on whether the
//! deployment scrapes metrics.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::Extension,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::get,
};
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry_sdk::metrics::{Aggregation, Instrument, InstrumentKind, SdkMeterProvider, Stream};
use prometheus::{Encoder, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    config::ServerConfig,
    error::{CommonError, Result},
};

/// Operational listener configuration.
///
/// The listener itself is unconditional: [`run_operational_server`] always binds
/// `host:port` and always serves `/health`, so a liveness probe does not depend
/// on whether the deployment scrapes metrics.
// TODO(high): one type, two responsibilities — `host`/`port` bind the listener that
// always serves HEALTH_PATH, `enabled`/`path` register the Prometheus endpoint. Split
// them and move the configuration key from `metrics` to `operational` (serde alias for
// the old one), in a single change across crates/icegate-{ingest,query,maintain}/src/config.rs,
// config/docker/{ingest,ingest-proxy,query,maintain}.yaml, config/helm/icegate
// (values.yaml, values.schema.json, templates/configmap-*.yaml) and the kustomize
// overlays. `ServerConfig::name` reports "Operational" while the operator edits a
// `metrics` section. Drop the `MetricsConfig` alias in `lib.rs` in the same change:
// it exists only for icegate-ee, which still imports the pre-rename name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationalConfig {
    /// Whether the `path` endpoint is registered.
    ///
    /// This flag governs metrics only. With it off the listener still runs and
    /// still answers `/health`; only the Prometheus endpoint is absent.
    #[serde(default)]
    pub enabled: bool,
    /// Bind host for the operational server.
    #[serde(default = "default_metrics_host")]
    pub host: String,
    /// Bind port for the operational server.
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    /// HTTP path for the metrics endpoint.
    #[serde(default = "default_metrics_path")]
    pub path: String,
}

impl Default for OperationalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: default_metrics_host(),
            port: default_metrics_port(),
            path: default_metrics_path(),
        }
    }
}

impl OperationalConfig {
    /// Validate metrics configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration value is invalid.
    pub fn validate(&self) -> Result<()> {
        // The host is checked whatever `enabled` says: the listener binds in
        // both cases, so an empty host is a configuration error here rather than
        // a bind failure minutes into startup.
        if self.host.trim().is_empty() {
            return Err(CommonError::Config("metrics host cannot be empty".to_string()));
        }

        if self.enabled {
            if self.path.trim().is_empty() {
                return Err(CommonError::Config("metrics path cannot be empty".to_string()));
            }

            if !self.path.starts_with('/') {
                return Err(CommonError::Config("metrics path must start with '/'".to_string()));
            }

            // The listener serves `HEALTH_PATH` unconditionally, so the same path
            // asked for here is a second `GET` route on it, and `Router::route`
            // answers that with a panic rather than an error. Refused here, where
            // the field that has to move is named.
            if self.path.trim() == HEALTH_PATH {
                return Err(CommonError::Config(format!(
                    "metrics path cannot be {HEALTH_PATH}: the operational listener always serves it"
                )));
            }
        }

        Ok(())
    }
}

impl ServerConfig for OperationalConfig {
    fn name(&self) -> &'static str {
        "Operational"
    }

    /// Always `true`: the operational listener occupies its port whatever
    /// `enabled` says, so [`check_port_conflicts`](crate::config::check_port_conflicts)
    /// has to see it.
    fn enabled(&self) -> bool {
        true
    }

    fn port(&self) -> u16 {
        self.port
    }
}

/// Metrics runtime state.
pub struct MetricsRuntime {
    registry: Registry,
    meter: Meter,
    meter_provider: SdkMeterProvider,
}

impl Drop for MetricsRuntime {
    fn drop(&mut self) {
        if let Err(err) = self.meter_provider.shutdown() {
            tracing::error!("Failed to shutdown meter provider: {err}");
        }
    }
}

impl MetricsRuntime {
    /// Create a new metrics runtime with a Prometheus exporter.
    ///
    /// # Errors
    ///
    /// Returns an error if the Prometheus exporter cannot be built.
    pub fn new(service_name: &'static str) -> Result<Self> {
        let registry = Registry::new();
        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .map_err(|err| CommonError::Config(format!("failed to build prometheus exporter: {err}")))?;
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(exporter)
            .with_view(histogram_view)
            .build();
        let meter = meter_provider.meter(service_name);

        opentelemetry::global::set_meter_provider(meter_provider.clone());
        opentelemetry_instrumentation_tokio::Config::new()
            .with_label("runtime.name", "main")
            .observe_current_runtime();

        Ok(Self {
            registry,
            meter,
            meter_provider,
        })
    }

    /// Return a clone of the service meter.
    #[must_use]
    pub fn meter(&self) -> Meter {
        self.meter.clone()
    }

    /// Return a shared registry for serving metrics.
    #[must_use]
    pub fn registry(&self) -> Arc<Registry> {
        Arc::new(self.registry.clone())
    }
}

const DURATION_BUCKETS: &[f64] = &[
    0.01, 0.03, 0.06, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.75, 1.0, 1.3, 1.6, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 30.0, 40.0,
    60.0, 120.0,
];
const VOLUME_BUCKETS: &[f64] = &[
    512.0,
    1_024.0,
    2_048.0,
    4_096.0,
    8_192.0,
    16_384.0,
    32_768.0,
    65_536.0,
    131_072.0,
    262_144.0,
    524_288.0,
    1_048_576.0,
    2_097_152.0,
    4_194_304.0,
    8_388_608.0,
    16_777_216.0,    // 16MB
    33_554_432.0,    // 32MB
    67_108_864.0,    // 64MB
    134_217_728.0,   // 128MB
    268_435_456.0,   // 256MB
    536_870_912.0,   // 512MB
    1_073_741_824.0, // 1GB
];
const COUNT_BUCKETS: &[f64] = &[
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
    30.0,
    40.0,
    50.0,
    75.0,
    100.0,
    200.0,
    500.0,
    1_000.0,
    2_000.0,
    5_000.0,
    10_000.0,
    20_000.0,
    30_000.0,
    50_000.0,
    75_000.0,
    100_000.0,
    200_000.0,
    500_000.0,
    1_000_000.0,
    10_000_000.0,
    100_000_000.0,
    500_000_000.0,
    1_000_000_000.0,
];

fn histogram_view(inst: &Instrument) -> Option<Stream> {
    if inst.kind() != InstrumentKind::Histogram {
        return None;
    }

    let boundaries: &[f64] = if inst.unit() == "s" || inst.name().contains("duration") {
        DURATION_BUCKETS
    } else if inst.unit() == "By" || inst.name().contains("bytes") {
        VOLUME_BUCKETS
    } else {
        COUNT_BUCKETS
    };

    Stream::builder()
        .with_aggregation(Aggregation::ExplicitBucketHistogram {
            boundaries: boundaries.to_vec(),
            record_min_max: true,
        })
        .build()
        .ok()
}

/// Path of the health endpoint served by every component's operational listener.
///
/// Named here because the Helm probes and the Compose health checks address it
/// by this literal; the deployment configs are the second copy, and they cite
/// this constant.
pub const HEALTH_PATH: &str = "/health";

/// Build the operational router: `/health` always, the Prometheus endpoint only
/// when a registry is given.
///
/// A `None` registry is what `metrics.enabled: false` means at this layer — the
/// component built no meter provider, so there is nothing to encode and the
/// `path` route is not registered at all.
fn build_operational_router(registry: Option<Arc<Registry>>, path: &str) -> Router {
    let mut router = Router::new().route(HEALTH_PATH, get(health_handler));
    if let Some(registry) = registry {
        router = router.route(path, get(metrics_handler)).layer(Extension(registry));
    }
    router
}

/// Run the operational server.
///
/// Binds `config.host:config.port` unconditionally and serves [`HEALTH_PATH`];
/// `registry` decides whether `config.path` is served alongside it. Returns when
/// `cancel_token` is cancelled and the listener has unbound.
///
/// # Errors
///
/// Returns an error if the address cannot be bound or the server stops with a
/// fatal error.
pub async fn run_operational_server(
    config: OperationalConfig,
    registry: Option<Arc<Registry>>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let addr = format!("{}:{}", config.host, config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let app = build_operational_router(registry, &config.path);

    tracing::info!("Operational server listening on {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("Operational server shutting down gracefully");
        })
        .await?;

    tracing::info!("Operational server stopped");

    Ok(())
}

/// Report that the process is up.
///
/// Liveness only: it answers as soon as the listener is bound, and says nothing
/// about the catalog, the WAL, or any worker pool. A probe that failed while a
/// component was recovering would have the kubelet kill it mid-recovery.
async fn health_handler() -> Response {
    // TODO(high): need to refactor the probes - now there is no understanding that ingest works
    Json(serde_json::json!({ "status": "ok" })).into_response()
}

async fn metrics_handler(Extension(registry): Extension<Arc<Registry>>) -> Response {
    let metric_families = registry.gather();
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    if encoder.encode(&metric_families, &mut buffer).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "failed to encode metrics").into_response();
    }

    String::from_utf8(buffer).map_or_else(
        |_| (StatusCode::INTERNAL_SERVER_ERROR, "invalid metrics encoding").into_response(),
        |body| ([(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body).into_response(),
    )
}

fn default_metrics_host() -> String {
    "127.0.0.1".to_string()
}

const fn default_metrics_port() -> u16 {
    9091
}

fn default_metrics_path() -> String {
    "/metrics".to_string()
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    /// Issue a `GET` against the operational router and return status and body.
    async fn get_path(router: Router, path: &str) -> (StatusCode, String) {
        let response = router
            .oneshot(Request::builder().uri(path).body(Body::empty()).expect("request"))
            .await
            .expect("response");
        let status = response.status();
        let body = response.into_body().collect().await.expect("body").to_bytes();
        (status, String::from_utf8(body.to_vec()).expect("utf-8 body"))
    }

    /// `HEALTH_PATH` as the metrics path would register a second `GET` route on
    /// the path the listener always serves, which `Router::route` reports by
    /// panicking. The configuration is refused instead.
    #[test]
    fn a_metrics_path_equal_to_the_health_path_is_refused() {
        let mut config = OperationalConfig {
            enabled: true,
            ..OperationalConfig::default()
        };

        // Baseline validates: any error after replacing only `path` therefore
        // comes from the health-path check rather than from the host or the
        // leading-slash rule.
        config.validate().expect("baseline operational config is valid");

        config.path = HEALTH_PATH.to_string();

        assert!(matches!(config.validate(), Err(CommonError::Config(_))));
    }

    #[tokio::test]
    async fn operational_server_serves_health_without_a_registry() {
        // `metrics.enabled: false` reaches this layer as a `None` registry: the
        // probe must still be answered, and `/metrics` must not exist.
        let router = build_operational_router(None, "/metrics");

        let (status, body) = get_path(router.clone(), HEALTH_PATH).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, r#"{"status":"ok"}"#);

        let (status, _) = get_path(router, "/metrics").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn operational_server_serves_metrics_when_a_registry_is_given() {
        let registry = Registry::new();
        let counter = prometheus::IntCounter::new("icegate_test_total", "test counter").expect("counter");
        registry.register(Box::new(counter.clone())).expect("register counter");
        counter.inc();

        let router = build_operational_router(Some(Arc::new(registry)), "/metrics");

        let (status, body) = get_path(router.clone(), "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.contains("icegate_test_total 1"),
            "the encoded exposition must carry the registered counter, got: {body}"
        );

        let (status, _) = get_path(router, HEALTH_PATH).await;
        assert_eq!(status, StatusCode::OK);
    }

    /// `enabled: false` governs the Prometheus endpoint alone, so the listener
    /// still takes its port. A port already held by the test is what makes the
    /// bind observable: a `run_operational_server` that returned early on the
    /// flag would report success and leave the probes without `/health`.
    #[tokio::test]
    async fn the_operational_server_binds_its_port_with_metrics_disabled() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the test holds the port the server must fail to take");
        let port = occupied.local_addr().expect("the bound address is known").port();

        let config = OperationalConfig {
            enabled: false,
            host: "127.0.0.1".to_string(),
            port,
            ..OperationalConfig::default()
        };

        // Bounded: a server that did take the port would serve until cancelled.
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_operational_server(config, None, CancellationToken::new()),
        )
        .await
        .expect("the server must return rather than serve on a port it cannot bind");

        assert!(matches!(outcome, Err(CommonError::Io(_))));
    }

    /// The host is validated outside the `enabled` branch, because the listener
    /// binds in both cases: an empty host is a configuration error rather than a
    /// failure to parse `":9091"` at startup.
    #[test]
    fn an_empty_host_is_refused_while_the_metrics_endpoint_is_disabled() {
        let mut config = OperationalConfig::default();
        assert!(!config.enabled, "the default leaves the metrics endpoint off");

        // Baseline validates: the error below therefore comes from the host rule
        // rather than from one of the rules the `enabled` branch holds.
        config.validate().expect("the default operational config is valid");

        config.host = String::new();

        assert!(matches!(config.validate(), Err(CommonError::Config(_))));
    }
}
