//! Pure memory-pressure guard: config, usage reader, sampler, and read handle.
//!
//! Framework-neutral (no axum/tonic). Progressive scaffold - the sampler body and
//! `MemoryPressure::spawn` land in the tasks that follow.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge};
use serde::{Deserialize, Serialize};

use crate::CancellationToken;
use crate::error::{CommonError, Result};
use crate::memory::cgroup;

/// Meter name for every instrument published by the guard.
const METER_NAME: &str = "icegate-memory-guard";

/// Runtime configuration for the process memory-pressure guard.
///
/// Mirrors `MetricsConfig`: every field has a `serde` default and `Default`
/// delegates to the same `default_*` fns, so the deserialize path and
/// `Default::default()` cannot drift apart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPressureConfig {
    /// Master switch. Default `true`; the guard is inert anyway when no finite
    /// cgroup limit exists, so leaving it on is safe in dev/CI.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Enter-pressure watermark: `working_set / limit`. Default `0.90`.
    #[serde(default = "default_high_watermark")]
    pub high_watermark: f64,
    /// Clear-pressure watermark. Default `0.85`.
    #[serde(default = "default_low_watermark")]
    pub low_watermark: f64,
    /// Sampler poll interval in milliseconds. Default `250`.
    #[serde(default = "default_sample_interval_ms")]
    pub sample_interval_ms: u64,
}

const fn default_enabled() -> bool {
    true
}
const fn default_high_watermark() -> f64 {
    0.90
}
const fn default_low_watermark() -> f64 {
    0.85
}
const fn default_sample_interval_ms() -> u64 {
    250
}

impl Default for MemoryPressureConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            high_watermark: default_high_watermark(),
            low_watermark: default_low_watermark(),
            sample_interval_ms: default_sample_interval_ms(),
        }
    }
}

impl MemoryPressureConfig {
    /// Validate the tunables. Does not require a cgroup to exist.
    ///
    /// # Errors
    /// [`CommonError::Config`] when the watermarks are not
    /// `0.0 < low_watermark < high_watermark <= 1.0`, or `sample_interval_ms == 0`.
    pub fn validate(&self) -> Result<()> {
        if !(self.low_watermark > 0.0 && self.low_watermark < self.high_watermark && self.high_watermark <= 1.0) {
            return Err(CommonError::Config(
                "memory_pressure watermarks must satisfy 0 < low < high <= 1".into(),
            ));
        }
        if self.sample_interval_ms == 0 {
            return Err(CommonError::Config(
                "memory_pressure sample_interval_ms must be > 0".into(),
            ));
        }
        Ok(())
    }
}

/// Source of the cgroup working-set sample. The concrete v1/v2 readers in
/// `cgroup.rs` resolve the fixed limit once at construction; each tick reads only
/// the numerator.
pub trait UsageReader: Send + Sync + 'static {
    /// Fixed cgroup memory limit in bytes. Guaranteed finite and `> 0` - an
    /// unlimited or zero limit yields no reader (an inert guard).
    fn limit_bytes(&self) -> u64;

    /// Current working-set numerator: `usage - inactive_file`, clamped at 0.
    ///
    /// # Errors
    /// Errors when the cgroup files cannot be read/parsed, including when the
    /// `inactive_file` stat key is absent (treated as a read error so the sampler
    /// retains its last state rather than over-counting with `inactive_file = 0`).
    fn read_working_set_bytes(&self) -> Result<u64>;
}

/// Cheap-clone read handle to the process memory-pressure signal.
/// Every read is one lock-free atomic load; safe on any request hot path.
#[derive(Clone)]
pub struct MemoryPressure {
    state: Arc<PressureState>,
}

/// Single-writer shared state. Exactly one sampler writes `pressured`/`ratio_bits`
/// per process; readers only load, so `Relaxed` ordering is sufficient.
struct PressureState {
    pressured: AtomicBool,
    ratio_bits: AtomicU64, // f64::to_bits(); NaN = "no sample / inert"
    shed_requests: Counter<u64>,
    inert_gauge: Gauge<u64>,
}

impl PressureState {
    fn new() -> Self {
        let meter = opentelemetry::global::meter(METER_NAME);
        Self {
            pressured: AtomicBool::new(false),
            ratio_bits: AtomicU64::new(f64::NAN.to_bits()),
            shed_requests: meter
                .u64_counter("icegate_memory_shed_requests")
                .with_description("requests rejected under memory pressure")
                .build(),
            inert_gauge: meter.u64_gauge("icegate_memory_guard_inert").build(),
        }
    }
}

impl MemoryPressure {
    /// Detect the platform cgroup reader and, when `config.enabled` and a finite
    /// limit exists, spawn the background sampler bound to `cancel_token`. Returns
    /// an inert handle (never sheds, no sampler) when disabled, unlimited, or the
    /// cgroup files are absent.
    ///
    /// When `/sys/fs/cgroup` exists but no finite limit resolves (likely a
    /// host-cgroupns/version misconfig), it `warn!`s and records
    /// `icegate_memory_guard_inert = 1` instead of a quiet `info!`, so ops can
    /// alert on a production guard that is silently protecting nothing.
    #[must_use]
    #[allow(clippy::needless_pass_by_value, clippy::option_if_let_else)]
    pub fn spawn(config: MemoryPressureConfig, cancel_token: CancellationToken) -> Self {
        if !config.enabled {
            return Self::inert();
        }
        match cgroup::detect_default_reader() {
            Some(reader) => {
                let sampler = MemoryPressureSampler::with_reader(&config, reader);
                let handle = sampler.handle();
                tokio::spawn(sampler.run(cancel_token));
                handle
            }
            None => {
                if cgroup::cgroupfs_present() {
                    tracing::warn!(
                        "memory pressure guard INERT despite cgroupfs present: no finite \
                         memory limit detected (misconfig? cgroupns=host? wrong cgroup \
                         version?). Requests will NOT be shed."
                    );
                    Self::inert_reported()
                } else {
                    tracing::info!("memory pressure guard disabled: no cgroup memory limit");
                    Self::inert()
                }
            }
        }
    }

    /// Inert handle that also records the `icegate_memory_guard_inert` gauge once.
    fn inert_reported() -> Self {
        let handle = Self::inert();
        handle.state.inert_gauge.record(1, &[]);
        handle
    }

    /// Inert handle: `is_under_pressure()` always false, `working_set_ratio()`
    /// always `None`. Used for disabled config and injected in tests/harnesses.
    ///
    /// The counter is still built (a no-op meter when no provider is set), so no
    /// `Option` wrapping and no test special-casing.
    #[must_use]
    pub fn inert() -> Self {
        Self {
            state: Arc::new(PressureState::new()),
        }
    }

    /// Whether the process is currently shedding.
    #[must_use]
    pub fn is_under_pressure(&self) -> bool {
        self.state.pressured.load(Ordering::Relaxed)
    }

    /// Last sampled `working_set / limit`, or `None` when inert / no sample yet.
    #[must_use]
    pub fn working_set_ratio(&self) -> Option<f64> {
        let ratio = f64::from_bits(self.state.ratio_bits.load(Ordering::Relaxed));
        (!ratio.is_nan()).then_some(ratio)
    }

    /// Record one shed on `surface` (`"loki"`, `"otlp_grpc"`, ...).
    pub fn record_shed(&self, surface: &'static str) {
        self.state.shed_requests.add(1, &[KeyValue::new("surface", surface)]);
    }
}

/// Owns the write side of the guard. `sample_once`/`run` are the production
/// sampling path and double as the deterministic test seam (no `#[cfg(test)]`
/// methods on the struct).
pub struct MemoryPressureSampler {
    reader: Arc<dyn UsageReader>,
    state: Arc<PressureState>,
    interval: Duration,
    high: f64,
    low: f64,
    ratio_gauge: Gauge<f64>,
    pressured_gauge: Gauge<u64>,
}

impl MemoryPressureSampler {
    /// Build a sampler over an injected reader (DI seam used in production and tests).
    #[must_use]
    pub fn with_reader(config: &MemoryPressureConfig, reader: Arc<dyn UsageReader>) -> Self {
        let meter = opentelemetry::global::meter(METER_NAME);
        Self {
            reader,
            state: Arc::new(PressureState::new()),
            // Clamp to a nonzero minimum so `run()`'s `tokio::time::interval` can never
            // panic on a `Duration::ZERO`. `MemoryPressureConfig::validate` already rejects
            // `sample_interval_ms == 0` at every in-repo config-load site; this is a
            // defense-in-depth backstop for direct programmatic callers of `spawn`, keeping
            // the guard fail-safe (a panicked sampler task would silently stop shedding).
            interval: Duration::from_millis(config.sample_interval_ms.max(1)),
            high: config.high_watermark,
            low: config.low_watermark,
            ratio_gauge: meter
                .f64_gauge("icegate_memory_working_set_ratio")
                .with_description("cgroup working-set bytes / limit")
                .build(),
            pressured_gauge: meter.u64_gauge("icegate_memory_pressured").build(),
        }
    }

    /// A cheap-clone read handle sharing this sampler's atomic state.
    #[must_use]
    pub fn handle(&self) -> MemoryPressure {
        MemoryPressure {
            state: Arc::clone(&self.state),
        }
    }

    /// One read + hysteresis update. `run` calls it every tick; tests drive it directly.
    ///
    /// # Errors
    /// Propagates the reader's error; on error the caller retains the prior flag
    /// and ratio (early return before any state store).
    pub fn sample_once(&self) -> Result<()> {
        let working_set = self.reader.read_working_set_bytes()?;
        #[allow(clippy::cast_precision_loss)]
        let ratio = working_set as f64 / self.reader.limit_bytes() as f64;
        self.state.ratio_bits.store(ratio.to_bits(), Ordering::Relaxed);
        self.ratio_gauge.record(ratio, &[]);

        let currently = self.state.pressured.load(Ordering::Relaxed);
        // Hysteresis: enter at >= high, stay until <= low. Any low < high gives a
        // nonzero deadband, so one sample can never cross both thresholds.
        let next = if currently {
            ratio > self.low
        } else {
            ratio >= self.high
        };
        if next != currently {
            self.state.pressured.store(next, Ordering::Relaxed);
            if next {
                tracing::warn!(ratio, "entering memory pressure - shedding requests");
            } else {
                tracing::info!(ratio, "memory pressure cleared");
            }
        }
        self.pressured_gauge.record(u64::from(next), &[]);
        Ok(())
    }

    /// Sampling loop until `cancel_token` fires. Read/parse errors are logged
    /// edge-triggered (warn on ok->err, debug while still failing) and the last
    /// flag is retained - never flips to pressured on a transient error, never
    /// panics, no `.unwrap()`/`.expect()`. Detached and cancel-driven: holds no
    /// joinable resource.
    pub async fn run(self, cancel_token: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_sample_ok = true;
        loop {
            tokio::select! {
                () = cancel_token.cancelled() => break,
                _ = ticker.tick() => match self.sample_once() {
                    Ok(()) => last_sample_ok = true,
                    Err(err) => {
                        if last_sample_ok {
                            tracing::warn!(%err, "memory pressure sample failed; retaining last state");
                        } else {
                            tracing::debug!(%err, "memory pressure sample still failing");
                        }
                        last_sample_ok = false;
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        assert!(MemoryPressureConfig::default().validate().is_ok());
    }

    #[test]
    fn rejects_low_ge_high() {
        let cfg = MemoryPressureConfig {
            high_watermark: 0.80,
            low_watermark: 0.90,
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(CommonError::Config(_))));
    }

    #[test]
    fn rejects_high_above_one() {
        let cfg = MemoryPressureConfig {
            high_watermark: 1.5,
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(CommonError::Config(_))));
    }

    #[test]
    fn rejects_zero_interval() {
        let cfg = MemoryPressureConfig {
            sample_interval_ms: 0,
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(CommonError::Config(_))));
    }

    #[test]
    fn rejects_nan_watermark() {
        let cfg = MemoryPressureConfig {
            high_watermark: f64::NAN,
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(CommonError::Config(_))));
    }

    #[test]
    fn inert_handle_never_pressured() {
        let handle = MemoryPressure::inert();
        assert!(!handle.is_under_pressure());
        assert_eq!(handle.working_set_ratio(), None);
    }

    use std::sync::atomic::AtomicU64;

    struct FixedReader {
        limit: u64,
        working_set: AtomicU64,
    }

    impl FixedReader {
        fn new(working_set: u64, limit: u64) -> Self {
            Self {
                limit,
                working_set: AtomicU64::new(working_set),
            }
        }
        fn set(&self, working_set: u64) {
            self.working_set.store(working_set, Ordering::Relaxed);
        }
    }

    impl UsageReader for FixedReader {
        fn limit_bytes(&self) -> u64 {
            self.limit
        }
        fn read_working_set_bytes(&self) -> Result<u64> {
            Ok(self.working_set.load(Ordering::Relaxed))
        }
    }

    #[test]
    fn enters_pressure_at_high_watermark_boundary() {
        // Pin explicit watermarks so this state-machine test is independent of the
        // tunable deployment defaults (which may change).
        let cfg = MemoryPressureConfig {
            high_watermark: 0.90,
            low_watermark: 0.85,
            ..Default::default()
        };
        let reader = Arc::new(FixedReader::new(90, 100));
        let sampler = MemoryPressureSampler::with_reader(&cfg, reader);
        let handle = sampler.handle();

        assert!(!handle.is_under_pressure());
        sampler.sample_once().expect("sample succeeds");
        assert!(handle.is_under_pressure());
        assert_eq!(handle.working_set_ratio(), Some(0.90));
    }

    #[test]
    fn hysteresis_retains_then_clears_then_reenters() {
        let cfg = MemoryPressureConfig {
            high_watermark: 0.90,
            low_watermark: 0.85,
            ..Default::default()
        };
        let reader = Arc::new(FixedReader::new(90, 100));
        let sampler = MemoryPressureSampler::with_reader(&cfg, reader.clone());
        let handle = sampler.handle();

        // Enter at the exact high boundary (0.90 >= 0.90).
        sampler.sample_once().expect("sample");
        assert!(handle.is_under_pressure());

        // 0.87 > low(0.85) while pressured -> stays.
        reader.set(87);
        sampler.sample_once().expect("sample");
        assert!(handle.is_under_pressure());

        // 0.85 > 0.85 is false -> clears.
        reader.set(85);
        sampler.sample_once().expect("sample");
        assert!(!handle.is_under_pressure());

        // 0.92 >= high -> pressured again.
        reader.set(92);
        sampler.sample_once().expect("sample");
        assert!(handle.is_under_pressure());
    }

    #[test]
    fn stays_clear_below_high_from_clear() {
        let cfg = MemoryPressureConfig {
            high_watermark: 0.90,
            low_watermark: 0.85,
            ..Default::default()
        };
        let reader = Arc::new(FixedReader::new(85, 100)); // 0.85 < 0.90 -> no enter
        let sampler = MemoryPressureSampler::with_reader(&cfg, reader);
        let handle = sampler.handle();

        sampler.sample_once().expect("sample");
        assert!(!handle.is_under_pressure());
    }

    use std::sync::atomic::AtomicBool;

    /// Reader that returns a fixed `Ok` until `fail()` is called, then errors.
    struct TogglingReader {
        limit: u64,
        working_set: u64,
        failing: AtomicBool,
    }

    impl TogglingReader {
        fn new(working_set: u64, limit: u64) -> Self {
            Self {
                limit,
                working_set,
                failing: AtomicBool::new(false),
            }
        }
        fn fail(&self) {
            self.failing.store(true, Ordering::Relaxed);
        }
    }

    impl UsageReader for TogglingReader {
        fn limit_bytes(&self) -> u64 {
            self.limit
        }
        fn read_working_set_bytes(&self) -> Result<u64> {
            if self.failing.load(Ordering::Relaxed) {
                Err(CommonError::Io(std::io::Error::from(std::io::ErrorKind::NotFound)))
            } else {
                Ok(self.working_set)
            }
        }
    }

    #[test]
    fn read_error_retains_pressured_flag() {
        let cfg = MemoryPressureConfig::default();
        let reader = Arc::new(TogglingReader::new(95, 100)); // 0.95 -> pressured
        let sampler = MemoryPressureSampler::with_reader(&cfg, reader.clone());
        let handle = sampler.handle();

        sampler.sample_once().expect("first sample ok");
        assert!(handle.is_under_pressure());

        reader.fail();
        assert!(matches!(sampler.sample_once(), Err(CommonError::Io(_))));
        // Flag retained across the error, not flipped clear.
        assert!(handle.is_under_pressure());
    }

    #[test]
    fn read_error_does_not_flip_clear_to_pressured() {
        let cfg = MemoryPressureConfig::default();
        let reader = Arc::new(TogglingReader::new(10, 100)); // 0.10 -> clear
        let sampler = MemoryPressureSampler::with_reader(&cfg, reader.clone());
        let handle = sampler.handle();

        sampler.sample_once().expect("first sample ok");
        assert!(!handle.is_under_pressure());

        reader.fail();
        assert!(matches!(sampler.sample_once(), Err(CommonError::Io(_))));
        assert!(!handle.is_under_pressure());
    }

    use crate::CancellationToken;

    #[test]
    fn disabled_config_yields_inert_handle() {
        let cfg = MemoryPressureConfig {
            enabled: false,
            ..Default::default()
        };
        // Disabled path returns inert() without a reader or a spawned task, so no
        // tokio runtime is required here.
        let handle = MemoryPressure::spawn(cfg, CancellationToken::new());
        assert!(!handle.is_under_pressure());
        assert_eq!(handle.working_set_ratio(), None);
    }
}
