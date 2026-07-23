//! Configuration for the LLM pricing crawler.

use serde::{Deserialize, Serialize};

use crate::compact::config::{CompactionJobsManagerConfig, JobsStorageConfig};
use crate::error::MaintainError;

/// Region sentinel for providers that publish one worldwide price.
pub const GLOBAL_REGION: &str = "global";

/// One upstream rate source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Stable identifier; must match a `PriceSource::name()` and is recorded in
    /// `prices.source`.
    pub name: String,
    /// Endpoint to fetch. Configurable rather than hardcoded so an air-gapped
    /// deployment can point at an internal mirror.
    pub url: String,
    /// Overrides the source's compiled-in provider ownership. Use this to hand a
    /// provider to a different source deliberately — for example giving
    /// `aws.bedrock` to litellm when the AWS endpoint is unreachable.
    #[serde(default)]
    pub owns: Option<Vec<String>>,
}

/// Configuration for the background pricing crawler.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PricingConfig {
    /// Whether the crawler runs. Disabled by default: enabling it gives the
    /// maintain pod outbound internet egress, which is a deployment decision.
    pub enabled: bool,
    /// Seconds between crawls.
    pub interval_secs: u64,
    /// Per-source HTTP timeout in seconds.
    pub timeout_secs: u64,
    /// Reject a rate that moved by more than this multiple of the live rate.
    /// `0.0` disables the check. See `guard::check_delta`.
    pub max_change_ratio: f64,
    /// Abort a source's rows when it returns fewer than this fraction of the
    /// models it currently has live — the signature of a truncated response.
    pub min_model_count_ratio: f64,
    /// Hard cap on a single source response body.
    pub max_response_bytes: usize,
    /// Sources to crawl.
    pub sources: Vec<SourceConfig>,
    /// Worker pool and job-state storage, shared shape with compaction and GC.
    /// The `Default` below sets a distinct `"pricing"` storage prefix; the
    /// discovery cadence comes from [`Self::interval_secs`], not from
    /// `jobsmanager.scan_interval_secs`, since this job has a single task.
    pub jobsmanager: CompactionJobsManagerConfig,
}

impl Default for PricingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: 21_600,
            timeout_secs: 60,
            max_change_ratio: 10.0,
            min_model_count_ratio: 0.8,
            max_response_bytes: 134_217_728,
            sources: vec![
                SourceConfig {
                    name: "openrouter".to_string(),
                    url: "https://openrouter.ai/api/v1/models".to_string(),
                    owns: None,
                },
                SourceConfig {
                    name: "litellm".to_string(),
                    url: "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
                        .to_string(),
                    owns: None,
                },
            ],
            // A distinct job-state prefix, for the same reason `GcConfig` sets
            // one: three jobmanagers share a bucket, and each has its own job
            // registry, so they must not share a prefix.
            jobsmanager: CompactionJobsManagerConfig {
                storage: JobsStorageConfig {
                    prefix: "pricing".to_string(),
                    ..JobsStorageConfig::default()
                },
                ..CompactionJobsManagerConfig::default()
            },
        }
    }
}

impl PricingConfig {
    /// Validate the crawler tunables.
    ///
    /// A disabled crawler validates trivially: `MaintainConfig` is shared with
    /// the one-shot `migrate` commands, which never set this block.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] when enabled and the interval or
    /// timeout is zero, no sources are configured, source names collide, the
    /// guard ratios are out of range, or the jobs-manager block is invalid.
    pub fn validate(&self) -> Result<(), MaintainError> {
        if !self.enabled {
            return Ok(());
        }
        if self.interval_secs == 0 {
            return Err(MaintainError::Config(
                "pricing.interval_secs must be greater than zero".to_string(),
            ));
        }
        if self.timeout_secs == 0 {
            return Err(MaintainError::Config(
                "pricing.timeout_secs must be greater than zero".to_string(),
            ));
        }
        if self.sources.is_empty() {
            return Err(MaintainError::Config(
                "pricing.sources must list at least one source when pricing is enabled".to_string(),
            ));
        }
        let mut seen: Vec<&str> = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            if seen.contains(&source.name.as_str()) {
                return Err(MaintainError::Config(format!(
                    "pricing.sources contains duplicate source name '{}'",
                    source.name
                )));
            }
            seen.push(&source.name);
        }
        if !(0.0..=1.0).contains(&self.min_model_count_ratio) {
            return Err(MaintainError::Config(
                "pricing.min_model_count_ratio must be between 0.0 and 1.0".to_string(),
            ));
        }
        if self.max_change_ratio < 0.0 {
            return Err(MaintainError::Config(
                "pricing.max_change_ratio must not be negative".to_string(),
            ));
        }
        // The crawler's worker pool and job-state storage, validated here for
        // the same reason `GcConfig::validate` validates its own: a zero
        // `worker_count` would otherwise start a jobmanager that reports
        // success and then never crawls.
        self.jobsmanager.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // `field_reassign_with_default` is allowed here because these tests build a
    // `PricingConfig::default()` and then override only the field(s) under test,
    // which is clearer than repeating the whole struct with update syntax.
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::field_reassign_with_default)]

    use super::PricingConfig;

    /// An enabled config whose jobs-state storage is populated enough to reach
    /// the crawler's own checks. `PricingConfig::validate` now also validates
    /// the `jobsmanager` block (as `GcConfig::validate` does), so a bare
    /// `default()` with `enabled = true` fails on the empty S3 endpoint before
    /// the tunable under test is ever reached.
    fn enabled_config() -> PricingConfig {
        let mut config = PricingConfig::default();
        config.enabled = true;
        config.jobsmanager.storage.endpoint = "http://localhost:9000".to_string();
        config.jobsmanager.storage.bucket = "warehouse".to_string();
        config
    }

    #[test]
    fn default_is_disabled() {
        // Disabled by default so an existing maintain deployment does not start
        // making outbound HTTP calls on upgrade.
        assert!(!PricingConfig::default().enabled);
    }

    #[test]
    fn validate_rejects_a_zero_interval() {
        let mut config = enabled_config();
        config.interval_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_a_zero_timeout() {
        // A zero timeout would make every source fail guard 1 on the first crawl.
        let mut config = enabled_config();
        config.timeout_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_an_out_of_range_model_count_ratio() {
        // The cardinality floor is a fraction of the live model count; a value
        // outside [0,1] either disables the guard or makes it unsatisfiable.
        for ratio in [-0.1, 1.5] {
            let mut config = enabled_config();
            config.min_model_count_ratio = ratio;
            assert!(
                config.validate().is_err(),
                "min_model_count_ratio {ratio} must be rejected"
            );
        }
    }

    #[test]
    fn validate_rejects_a_negative_change_ratio() {
        // Zero disables the delta guard deliberately; negative is meaningless and
        // would reject every rate change.
        let mut config = enabled_config();
        config.max_change_ratio = -1.0;
        assert!(config.validate().is_err());

        config.max_change_ratio = 0.0;
        assert!(
            config.validate().is_ok(),
            "zero disables the guard rather than being invalid"
        );
    }

    #[test]
    fn validate_rejects_enabled_with_no_sources() {
        let mut config = enabled_config();
        config.sources.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_duplicate_source_names() {
        let mut config = enabled_config();
        let first = config.sources[0].clone();
        config.sources.push(first);
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_an_invalid_jobsmanager_block() {
        // The crawler's worker pool is as load-bearing as its tunables: a zero
        // `worker_count` starts a jobmanager that reports success and never
        // crawls, so it must fail at load like `GcConfig` does.
        let mut config = enabled_config();
        config.jobsmanager.worker_count = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn default_uses_a_job_state_prefix_of_its_own() {
        // Three jobmanagers share one bucket and each has its own job registry;
        // reusing compaction's "compactor" prefix would mix their job state.
        assert_eq!(PricingConfig::default().jobsmanager.storage.prefix, "pricing");
    }

    #[test]
    fn disabled_config_skips_validation() {
        // `migrate` shares MaintainConfig and never sets [pricing]; a disabled
        // block must never block config load.
        let mut config = PricingConfig::default();
        config.sources.clear();
        config.interval_secs = 0;
        assert!(config.validate().is_ok());
    }
}
