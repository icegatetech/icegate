//! OpenTelemetry tracing configuration and utilities.
//!
//! Provides shared tracing configuration and initialization for IceGate services.
//! Uses OTLP exporter with tonic to send traces to Jaeger or other OTLP-compatible backends.

use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, SdkTracerProvider};
use serde::{Deserialize, Serialize};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::error::{CommonError, Result};

/// `OpenTelemetry` tracing configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Whether tracing is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Service name for trace identification
    #[serde(default = "default_service_name")]
    pub service_name: String,

    /// OTLP endpoint for trace export (e.g., `http://jaeger:4317`)
    /// Falls back to `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable
    #[serde(default)]
    pub otlp_endpoint: Option<String>,

    /// Trace sampling ratio (0.0 to 1.0)
    /// - 1.0 = 100% sampling (all traces)
    /// - 0.0 = 0% sampling (no traces)
    /// - 0.1 = 10% sampling
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            service_name: default_service_name(),
            otlp_endpoint: None,
            sample_ratio: default_sample_ratio(),
        }
    }
}

const fn default_enabled() -> bool {
    true
}

fn default_service_name() -> String {
    "icegate".to_string()
}

const fn default_sample_ratio() -> f64 {
    1.0
}

impl TracingConfig {
    /// Validate the tracing configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The sample ratio is not between 0.0 and 1.0
    /// - Tracing is enabled but no OTLP endpoint is configured
    pub fn validate(&self) -> Result<()> {
        if self.sample_ratio < 0.0 || self.sample_ratio > 1.0 {
            return Err(CommonError::Config(format!(
                "Tracing sample_ratio must be between 0.0 and 1.0, got {}",
                self.sample_ratio
            )));
        }

        if self.enabled && self.otlp_endpoint.is_none() && std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
            return Err(CommonError::Config(
                "Tracing is enabled but no OTLP endpoint is configured. \
                 Set tracing.otlp_endpoint in config or OTEL_EXPORTER_OTLP_ENDPOINT environment variable"
                    .to_string(),
            ));
        }

        Ok(())
    }

    /// Get the OTLP endpoint, falling back to environment variable.
    fn get_otlp_endpoint(&self) -> Option<String> {
        self.otlp_endpoint
            .clone()
            .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
    }
}

/// RAII guard for tracing initialization.
///
/// Ensures proper shutdown of the tracer provider when dropped.
/// Keep this guard alive for the lifetime of the application.
pub struct TracingGuard {
    provider: Option<SdkTracerProvider>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(e) = provider.shutdown() {
                tracing::error!("Failed to shutdown tracer provider: {e}");
            }
        }
    }
}

/// Initialize `OpenTelemetry` tracing with the given configuration.
///
/// Sets up an OTLP exporter with tonic, configures sampling based on the sample ratio,
/// and initializes the tracing subscriber with an `OpenTelemetry` layer.
///
/// # Arguments
///
/// * `config` - Tracing configuration
///
/// # Returns
///
/// Returns a `TracingGuard` that must be kept alive for the lifetime of the application.
/// When the guard is dropped, it will flush any remaining traces and shut down the tracer provider.
///
/// # Errors
///
/// Returns an error if:
/// - Tracing is enabled but configuration is invalid
/// - Failed to initialize the OTLP exporter
/// - Failed to build the tracer provider
///
/// # Examples
///
/// ```no_run
/// use icegate_common::{TracingConfig, init_tracing};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = TracingConfig {
///     enabled: true,
///     service_name: "icegate-ingest".to_string(),
///     otlp_endpoint: Some("http://jaeger:4317".to_string()),
///     sample_ratio: 1.0,
/// };
///
/// let _guard = init_tracing(&config)?;
///
/// // Use tracing macros
/// tracing::info!("Application started");
///
/// // Keep _guard alive until application shutdown
/// # Ok(())
/// # }
/// ```
pub fn init_tracing(config: &TracingConfig) -> Result<TracingGuard> {
    // If tracing is disabled, just initialize basic logging without OpenTelemetry
    if !config.enabled {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .try_init()
            .ok();

        return Ok(TracingGuard { provider: None });
    }

    // Get OTLP endpoint
    let endpoint = config
        .get_otlp_endpoint()
        .ok_or_else(|| CommonError::Config("No OTLP endpoint configured for tracing".to_string()))?;

    // Configure sampler based on sample ratio
    let sampler = if config.sample_ratio >= 1.0 {
        Sampler::AlwaysOn
    } else if config.sample_ratio <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.sample_ratio)
    };

    // Create OTLP exporter
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| CommonError::Config(format!("Failed to create OTLP exporter: {e}")))?;

    // Build tracer provider with batch exporter
    let service_name = config.service_name.clone();
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(Resource::builder_empty().with_service_name(service_name).build())
        .build();

    // Set global tracer provider
    global::set_tracer_provider(tracer_provider.clone());
    let tracer = global::tracer(config.service_name.clone());

    // Initialize tracing subscriber with OpenTelemetry layer
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .ok();

    Ok(TracingGuard {
        provider: Some(tracer_provider),
    })
}
