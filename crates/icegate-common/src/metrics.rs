//! Prometheus metrics utilities.

use std::sync::Arc;

use axum::{
    Router,
    extract::Extension,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::get,
};
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    config::ServerConfig,
    error::{CommonError, Result},
};

/// Metrics server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Whether metrics are enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Bind host for the standalone metrics server.
    #[serde(default = "default_metrics_host")]
    pub host: String,
    /// Bind port for the standalone metrics server.
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    /// HTTP path for the metrics endpoint.
    #[serde(default = "default_metrics_path")]
    pub path: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: default_metrics_host(),
            port: default_metrics_port(),
            path: default_metrics_path(),
        }
    }
}

impl MetricsConfig {
    /// Validate metrics configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration value is invalid.
    pub fn validate(&self) -> Result<()> {
        if self.enabled {
            if self.host.trim().is_empty() {
                return Err(CommonError::Config("metrics host cannot be empty".to_string()));
            }

            if self.path.trim().is_empty() {
                return Err(CommonError::Config("metrics path cannot be empty".to_string()));
            }

            if !self.path.starts_with('/') {
                return Err(CommonError::Config("metrics path must start with '/'".to_string()));
            }
        }

        Ok(())
    }
}

impl ServerConfig for MetricsConfig {
    fn name(&self) -> &'static str {
        "Metrics"
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn port(&self) -> u16 {
        self.port
    }
}

/// Metrics runtime state.
pub struct MetricsRuntime {
    _meter_provider: SdkMeterProvider,
    registry: Registry,
    meter: Meter,
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
        let meter_provider = SdkMeterProvider::builder().with_reader(exporter).build();
        let meter = meter_provider.meter(service_name);

        Ok(Self {
            _meter_provider: meter_provider,
            registry,
            meter,
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

/// Build a router serving Prometheus metrics.
fn metrics_router(registry: Arc<Registry>, path: &str) -> Router {
    Router::new().route(path, get(metrics_handler)).layer(Extension(registry))
}

/// Run a standalone metrics server.
pub async fn run_metrics_server(
    config: MetricsConfig,
    registry: Arc<Registry>,
    cancel_token: CancellationToken,
) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    let addr = format!("{}:{}", config.host, config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let app = metrics_router(registry, &config.path);

    tracing::info!("Metrics server listening on {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("Metrics server shutting down gracefully");
        })
        .await?;

    tracing::info!("Metrics server stopped");

    Ok(())
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
