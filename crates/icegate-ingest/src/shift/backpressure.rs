//! PI-based backpressure controller for shift overload protection.
//!
//! Measures shift iteration wall-clock time relative to the configured iteration
//! interval and computes a rejection probability. OTLP handlers read this
//! probability via a shared [`AtomicU32`] and probabilistically reject requests
//! with 429 / RESOURCE_EXHAUSTED.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::IngestError;

/// Configuration for PI-based backpressure control.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BackpressureConfig {
    /// Enable backpressure control. When false, rejection probability is always 0.
    pub enabled: bool,
    /// Target load ratio (`iteration_duration` / `iteration_interval`). Range: (0.0, 1.0].
    pub setpoint: f64,
    /// Maximum rejection probability. Range: (0.0, 1.0].
    pub max_rejection_probability: f64,
    /// Proportional gain.
    pub kp: f64,
    /// Integral gain.
    pub ki: f64,
    /// Anti-windup clamp for the integral term.
    pub integral_max: f64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            setpoint: 0.8,
            max_rejection_probability: 0.95,
            kp: 0.07,
            ki: 0.01,
            integral_max: 2.0,
        }
    }
}

impl BackpressureConfig {
    /// Validate configuration values.
    ///
    /// # Errors
    ///
    /// Returns [`IngestError::Config`] if any value is out of range.
    pub fn validate(&self) -> Result<(), IngestError> {
        if self.setpoint <= 0.0 || self.setpoint > 1.0 {
            return Err(IngestError::Config(
                "backpressure.setpoint must be in (0.0, 1.0]".to_string(),
            ));
        }
        if self.max_rejection_probability <= 0.0 || self.max_rejection_probability > 1.0 {
            return Err(IngestError::Config(
                "backpressure.max_rejection_probability must be in (0.0, 1.0]".to_string(),
            ));
        }
        if self.kp < 0.0 {
            return Err(IngestError::Config("backpressure.kp must be non-negative".to_string()));
        }
        if self.ki < 0.0 {
            return Err(IngestError::Config("backpressure.ki must be non-negative".to_string()));
        }
        if self.integral_max <= 0.0 {
            return Err(IngestError::Config(
                "backpressure.integral_max must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

/// Encode a probability (0.0..=1.0) as a `u32` with 0.01% resolution.
///
/// Uses fixed-point encoding: 10000 = 100%, 0 = 0%.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "value is clamped to [0.0, 10_000.0] before cast"
)]
pub fn encode_probability(p: f64) -> u32 {
    (p * 10_000.0).round().clamp(0.0, 10_000.0) as u32
}

/// Decode a `u32`-encoded probability back to `f64`.
#[allow(dead_code, reason = "used in Task 7/8: OTLP handler backpressure checks")]
pub fn decode_probability(encoded: u32) -> f64 {
    f64::from(encoded) / 10_000.0
}

/// PI controller that computes rejection probability from shift iteration duration.
///
/// Owns the PI state (integral accumulator) and writes the current rejection
/// probability to a shared [`AtomicU32`]. OTLP handlers read this atomic
/// lock-free to decide whether to reject incoming requests.
pub struct BackpressureController {
    config: BackpressureConfig,
    iteration_interval_ms: u64,
    integral: f64,
    current_probability: f64,
    last_error: f64,
    last_load_ratio: f64,
    rejection_probability: Arc<AtomicU32>,
}

impl BackpressureController {
    /// Create a new controller.
    ///
    /// # Arguments
    ///
    /// * `config` - PI tuning parameters
    /// * `iteration_interval_ms` - The configured shift iteration interval in milliseconds
    pub fn new(config: BackpressureConfig, iteration_interval_ms: u64) -> Self {
        Self {
            config,
            iteration_interval_ms,
            integral: 0.0,
            current_probability: 0.0,
            last_error: 0.0,
            last_load_ratio: 0.0,
            rejection_probability: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Create a new controller with a pre-existing shared atomic.
    ///
    /// Use this when the [`AtomicU32`] is created externally and shared
    /// with OTLP handlers.
    #[allow(dead_code, reason = "alternative constructor for external AtomicU32 sharing")]
    pub const fn with_shared_atomic(
        config: BackpressureConfig,
        iteration_interval_ms: u64,
        rejection_probability: Arc<AtomicU32>,
    ) -> Self {
        Self {
            config,
            iteration_interval_ms,
            integral: 0.0,
            current_probability: 0.0,
            last_error: 0.0,
            last_load_ratio: 0.0,
            rejection_probability,
        }
    }

    /// Get a clone of the shared rejection probability atomic.
    pub fn rejection_probability_atomic(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.rejection_probability)
    }

    /// Update the controller with a new iteration duration measurement.
    ///
    /// Computes the PI output and writes the new rejection probability to the
    /// shared [`AtomicU32`].
    #[allow(
        clippy::cast_precision_loss,
        reason = "acceptable precision for load ratio calculation"
    )]
    pub fn update(&mut self, iteration_duration: Duration) {
        if !self.config.enabled {
            return;
        }

        self.last_load_ratio = iteration_duration.as_millis() as f64 / self.iteration_interval_ms as f64;
        self.last_error = self.last_load_ratio - self.config.setpoint;

        // Anti-windup: clamp integral accumulator
        self.integral = (self.integral + self.last_error).clamp(-self.config.integral_max, self.config.integral_max);

        // PI output
        let raw = self.config.kp.mul_add(self.last_error, self.config.ki * self.integral);
        self.current_probability = raw.clamp(0.0, self.config.max_rejection_probability);

        self.rejection_probability
            .store(encode_probability(self.current_probability), Ordering::Relaxed);
    }

    /// Get the current rejection probability (for testing/metrics).
    pub const fn rejection_probability(&self) -> f64 {
        self.current_probability
    }

    /// Get the current integral accumulator value (for metrics).
    pub const fn integral(&self) -> f64 {
        self.integral
    }

    /// Get the last computed error signal (for metrics).
    pub const fn last_error(&self) -> f64 {
        self.last_error
    }

    /// Get the last computed load ratio (for metrics).
    pub const fn last_load_ratio(&self) -> f64 {
        self.last_load_ratio
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> BackpressureConfig {
        BackpressureConfig::default()
    }

    #[test]
    fn default_config_values() {
        let config = BackpressureConfig::default();
        assert!(config.enabled);
        assert!((config.setpoint - 0.8).abs() < f64::EPSILON);
        assert!((config.max_rejection_probability - 0.95).abs() < f64::EPSILON);
        assert!((config.kp - 0.07).abs() < f64::EPSILON);
        assert!((config.ki - 0.01).abs() < f64::EPSILON);
        assert!((config.integral_max - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn controller_starts_with_zero_rejection() {
        let controller = BackpressureController::new(default_config(), 30_000);
        assert!(
            controller.rejection_probability().abs() < f64::EPSILON,
            "Expected zero rejection"
        );
    }

    #[test]
    fn no_rejection_when_under_setpoint() {
        let mut controller = BackpressureController::new(default_config(), 30_000);
        // 50% load (15s of 30s) with setpoint 0.8 -> error = -0.3 -> p = 0.0
        controller.update(Duration::from_millis(15_000));
        assert!(
            controller.rejection_probability().abs() < f64::EPSILON,
            "Expected zero rejection"
        );
    }

    #[test]
    fn rejection_when_over_setpoint() {
        let mut controller = BackpressureController::new(default_config(), 30_000);
        // 100% load (30s of 30s) with setpoint 0.8 -> error = 0.2
        // p = Kp * 0.2 + Ki * 0.2 = 0.07 * 0.2 + 0.01 * 0.2 = 0.016
        controller.update(Duration::from_millis(30_000));
        let p = controller.rejection_probability();
        assert!(p > 0.0, "Expected positive rejection, got {p}");
        assert!((p - 0.016).abs() < 0.001, "Expected ~0.016, got {p}");
    }

    #[test]
    fn rejection_capped_at_max() {
        let config = BackpressureConfig {
            max_rejection_probability: 0.1,
            ..default_config()
        };
        let mut controller = BackpressureController::new(config, 30_000);
        // Extreme overload: 300% load → unclamped p ~0.174, capped at 0.1
        controller.update(Duration::from_millis(90_000));
        let p = controller.rejection_probability();
        assert!((p - 0.1).abs() < f64::EPSILON, "Expected capped at 0.1, got {p}");
    }

    #[test]
    fn integral_accumulates_over_iterations() {
        let mut controller = BackpressureController::new(default_config(), 30_000);
        // Sustained 100% load -> error = 0.2 each iteration
        controller.update(Duration::from_millis(30_000));
        let p1 = controller.rejection_probability();
        controller.update(Duration::from_millis(30_000));
        let p2 = controller.rejection_probability();
        assert!(p2 > p1, "Integral should increase p: p1={p1}, p2={p2}");
    }

    #[test]
    fn integral_has_anti_windup() {
        let mut controller = BackpressureController::new(default_config(), 30_000);
        // Push integral to its limit
        for _ in 0..100 {
            controller.update(Duration::from_millis(90_000));
        }
        let p = controller.rejection_probability();
        // Max from integral: Ki * integral_max = 0.01 * 2.0 = 0.02
        // Max from proportional at 3.0 load: Kp * (3.0 - 0.8) = 0.07 * 2.2 = 0.154
        // Total: 0.174, below max_rejection_probability (0.95)
        assert!((p - 0.174).abs() < 0.001, "Expected ~0.174, got {p}");
        // Verify integral is clamped at integral_max
        assert!((controller.integral() - 2.0).abs() < f64::EPSILON, "Integral should be clamped");
    }

    #[test]
    fn recovery_after_overload() {
        let mut controller = BackpressureController::new(default_config(), 30_000);
        // Overload
        controller.update(Duration::from_millis(30_000));
        assert!(controller.rejection_probability() > 0.0);
        // Recover: 0% load for many iterations
        for _ in 0..50 {
            controller.update(Duration::from_millis(0));
        }
        assert!(
            controller.rejection_probability().abs() < f64::EPSILON,
            "Should recover to zero"
        );
    }

    #[test]
    fn disabled_controller_always_returns_zero() {
        let config = BackpressureConfig {
            enabled: false,
            ..default_config()
        };
        let mut controller = BackpressureController::new(config, 30_000);
        controller.update(Duration::from_millis(90_000));
        assert!(
            controller.rejection_probability().abs() < f64::EPSILON,
            "Disabled controller should have zero rejection"
        );
    }

    #[test]
    fn atomic_probability_encoding() {
        let mut controller = BackpressureController::new(default_config(), 30_000);
        controller.update(Duration::from_millis(30_000));
        let p = controller.rejection_probability();
        let encoded = encode_probability(p);
        let decoded = decode_probability(encoded);
        assert!((p - decoded).abs() < 0.0002, "Round-trip error too large");
    }

    #[test]
    fn config_validation_rejects_invalid_setpoint() {
        let config = BackpressureConfig {
            setpoint: 0.0,
            ..default_config()
        };
        assert!(config.validate().is_err());

        let config = BackpressureConfig {
            setpoint: 1.5,
            ..default_config()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_validation_rejects_negative_gains() {
        let config = BackpressureConfig {
            kp: -0.1,
            ..default_config()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_validation_accepts_defaults() {
        assert!(default_config().validate().is_ok());
    }
}
