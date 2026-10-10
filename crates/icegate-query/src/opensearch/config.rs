//! `OpenSearch` listener configuration.

use std::fmt;

use icegate_common::ServerConfig;
use serde::{Deserialize, Serialize};

use crate::error::{QueryError, Result};

/// Default bind address.
const DEFAULT_HOST: &str = "0.0.0.0";
/// Default listener port.
const DEFAULT_PORT: u16 = 9200;
/// Version reported when none is configured.
const DEFAULT_VERSION: &str = "3.9.0";
/// Cluster name reported when none is configured.
const DEFAULT_CLUSTER_NAME: &str = "icegate";
/// Default request body limit, in bytes.
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;
/// Default field-discovery window, in seconds.
const DEFAULT_FIELD_DISCOVERY_WINDOW_SECS: u64 = 24 * 60 * 60;
/// The only major version IceGate emulates.
const EMULATED_MAJOR_VERSION: u32 = 3;

/// The `OpenSearch`-compatible listener.
///
/// Disabled by default, like Flight SQL: an absent block leaves the port closed instead of
/// opening a listener that authenticates no caller.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenSearchConfig {
    /// Whether the listener runs.
    pub enabled: bool,
    /// Bind address.
    pub host: String,
    /// Bind port.
    pub port: u16,
    /// Version reported by `GET /` and `/_nodes`: a canonical `MAJOR.MINOR.PATCH` (ASCII digits,
    /// no leading zeros, no pre-release or build suffix, no `v` prefix) with major 3. Any other
    /// value is refused at startup.
    pub version: String,
    /// Cluster name reported by `GET /`, `/_nodes` and `/_cluster/health`. It seeds the stable
    /// cluster and node identifiers, so changing it changes them.
    pub cluster_name: String,
    /// Largest request body the listener accepts. It also bounds how much of a refused request's
    /// body is drained, so the client reads the refusal instead of a reset.
    pub max_request_body_bytes: usize,
    /// How far back, in seconds, field discovery (`_field_caps`, `_mapping`) looks for
    /// attribute-map keys. A key seen only before the window, or only in the WAL, is not listed;
    /// a path naming it still resolves.
    pub field_discovery_window_secs: u64,
}

impl OpenSearchConfig {
    /// Validate the section. Nothing is checked while it is disabled, so a deployment that never
    /// enables it cannot fail on it.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Config`] for a blank host or cluster name, a zero
    /// `max_request_body_bytes` or `field_discovery_window_secs`, or a
    /// [`version`](Self::version) outside its documented form.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.host.trim().is_empty() {
            return Err(QueryError::Config("OpenSearch host cannot be empty".into()));
        }
        if self.cluster_name.trim().is_empty() {
            return Err(QueryError::Config("OpenSearch cluster_name cannot be empty".into()));
        }
        if self.max_request_body_bytes == 0 {
            return Err(QueryError::Config(
                "OpenSearch max_request_body_bytes must be greater than zero".into(),
            ));
        }
        if self.field_discovery_window_secs == 0 {
            return Err(QueryError::Config(
                "OpenSearch field_discovery_window_secs must be greater than zero".into(),
            ));
        }
        EmulatedVersion::parse(&self.version).map(|_| ())
    }
}

impl Default for OpenSearchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            version: DEFAULT_VERSION.to_string(),
            cluster_name: DEFAULT_CLUSTER_NAME.to_string(),
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            field_discovery_window_secs: DEFAULT_FIELD_DISCOVERY_WINDOW_SECS,
        }
    }
}

impl ServerConfig for OpenSearchConfig {
    fn name(&self) -> &'static str {
        "OpenSearch"
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn port(&self) -> u16 {
        self.port
    }
}

/// A version IceGate reports as its own: a canonical `MAJOR.MINOR.PATCH` of the emulated major.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EmulatedVersion {
    major: u32,
    minor: u32,
    patch: u32,
}

impl EmulatedVersion {
    /// Parse `text`.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Config`] when `text` is not three dot-separated canonical numbers
    /// (ASCII digits, no leading zero unless the number is `0`, within `u32`) or its major is not
    /// the emulated major.
    pub(crate) fn parse(text: &str) -> Result<Self> {
        let build_version_error = || {
            QueryError::Config(format!(
                "OpenSearch version {text:?} must be a canonical MAJOR.MINOR.PATCH with major \
                 {EMULATED_MAJOR_VERSION} and no pre-release or build suffix"
            ))
        };
        let mut parts = text.split('.');
        let (Some(major), Some(minor), Some(patch), None) = (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(build_version_error());
        };
        let (Some(major), Some(minor), Some(patch)) = (
            parse_version_number(major),
            parse_version_number(minor),
            parse_version_number(patch),
        ) else {
            return Err(build_version_error());
        };
        if major != EMULATED_MAJOR_VERSION {
            return Err(build_version_error());
        }
        Ok(Self { major, minor, patch })
    }
}

impl fmt::Display for EmulatedVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// One canonical version number: ASCII digits only, no leading zero unless it is `0`, within
/// `u32`.
fn parse_version_number(part: &str) -> Option<u32> {
    let is_canonical =
        !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()) && (part == "0" || !part.starts_with('0'));
    if is_canonical { part.parse().ok() } else { None }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn build_enabled_config() -> OpenSearchConfig {
        OpenSearchConfig {
            enabled: true,
            ..OpenSearchConfig::default()
        }
    }

    #[test]
    fn default_is_disabled_on_9200() {
        let config = OpenSearchConfig::default();

        assert!(!config.enabled);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 9200);
        assert_eq!(config.version, "3.9.0");
        assert_eq!(config.cluster_name, "icegate");
        assert_eq!(config.max_request_body_bytes, 33_554_432);
        assert_eq!(config.field_discovery_window_secs, 86_400);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_accepts_canonical_versions_of_major_3() {
        // The last one carries `u32::MAX` as its minor, the largest number a field holds.
        for version in ["3.0.0", "3.9.0", "3.10.12", "3.4294967295.0"] {
            let config = OpenSearchConfig {
                version: version.to_string(),
                ..build_enabled_config()
            };

            assert!(config.validate().is_ok(), "{version} must be accepted");
        }
    }

    /// IceGate reports only a canonical `MAJOR.MINOR.PATCH` of major 3, so every other form is
    /// refused at startup instead of being reported.
    #[test]
    fn validate_rejects_versions_outside_canonical_major_3() {
        for version in [
            "",
            "3.9",
            "3.9.0.0",
            "3.9.0-SNAPSHOT",
            "3.9.0+build",
            "v3.9.0",
            "03.9.0",
            "3.09.0",
            "3.9.x",
            "3..0",
            // An integer parse alone accepts a leading plus sign; only ASCII digits are canonical.
            "3.+9.0",
            // A minor one past `u32::MAX`: canonical digits that no version number can hold.
            "3.4294967296.0",
            "2.19.0",
            "4.0.0",
            // Only 3.x is emulated.
            "7.10.2",
        ] {
            let config = OpenSearchConfig {
                version: version.to_string(),
                ..build_enabled_config()
            };

            assert!(
                matches!(config.validate(), Err(QueryError::Config(_))),
                "{version:?} must be refused"
            );
        }
    }

    #[test]
    fn validate_rejects_empty_host_cluster_name_and_zero_limits_when_enabled() {
        let cases: [(&str, fn(&mut OpenSearchConfig)); 4] = [
            ("host", |config| config.host = " ".to_string()),
            ("cluster_name", |config| config.cluster_name = String::new()),
            ("max_request_body_bytes", |config| config.max_request_body_bytes = 0),
            ("field_discovery_window_secs", |config| {
                config.field_discovery_window_secs = 0;
            }),
        ];
        for (field, break_field) in cases {
            let mut config = build_enabled_config();
            break_field(&mut config);

            assert!(matches!(config.validate(), Err(QueryError::Config(_))), "{field}");
        }
    }

    /// The zero refusal stops at zero: the smallest non-zero limits are accepted.
    #[test]
    fn validate_accepts_limits_of_one_when_enabled() {
        let config = OpenSearchConfig {
            max_request_body_bytes: 1,
            field_discovery_window_secs: 1,
            ..build_enabled_config()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_ignores_a_broken_section_while_disabled() {
        let config = OpenSearchConfig {
            host: String::new(),
            cluster_name: String::new(),
            max_request_body_bytes: 0,
            field_discovery_window_secs: 0,
            version: "not-a-version".to_string(),
            ..OpenSearchConfig::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn emulated_version_renders_as_parsed() {
        for text in ["3.10.2", "3.4294967295.0"] {
            let version = EmulatedVersion::parse(text).expect("canonical version");

            assert_eq!(version.to_string(), text);
        }
    }

    /// The section deserializes with every key optional, so a field renamed away from the key the
    /// chart renders would leave the operator's value ignored instead of refused.
    #[test]
    fn every_key_deserializes_under_its_snake_case_name() {
        let config = serde_json::from_value::<OpenSearchConfig>(json!({
            "enabled": true,
            "host": "127.0.0.1",
            "port": 9201,
            "version": "3.10.1",
            "cluster_name": "c1",
            "max_request_body_bytes": 1_048_577,
            "field_discovery_window_secs": 3601,
        }))
        .expect("a section naming every key deserializes");

        assert!(config.enabled);
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 9201);
        assert_eq!(config.version, "3.10.1");
        assert_eq!(config.cluster_name, "c1");
        assert_eq!(config.max_request_body_bytes, 1_048_577);
        assert_eq!(config.field_discovery_window_secs, 3601);
    }

    #[test]
    fn a_section_naming_only_enabled_takes_every_other_key_from_the_defaults() {
        let config = serde_json::from_value::<OpenSearchConfig>(json!({ "enabled": true }))
            .expect("a section naming one key deserializes");

        assert!(config.enabled);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 9200);
        assert_eq!(config.version, "3.9.0");
        assert_eq!(config.cluster_name, "icegate");
        assert_eq!(config.max_request_body_bytes, 33_554_432);
        assert_eq!(config.field_discovery_window_secs, 86_400);
    }
}
