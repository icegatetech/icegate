//! `OpenSearch` listener configuration.

use std::fmt;

use icegate_common::ServerConfig;
use serde::{Deserialize, Serialize};

use super::system_index::parse_s3_location;
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
/// Default of [`OpenSearchConfig::max_query_memory_bytes`].
const DEFAULT_MAX_QUERY_MEMORY_BYTES: usize = 1024 * 1024 * 1024;
/// Default field-discovery window, in seconds.
const DEFAULT_FIELD_DISCOVERY_WINDOW_SECS: u64 = 24 * 60 * 60;
/// Default `max_trace_lookback_secs`.
const DEFAULT_MAX_TRACE_LOOKBACK_SECS: u64 = 3_600;
/// Default `max_cross_row_traces`.
const DEFAULT_MAX_CROSS_ROW_TRACES: usize = 1_000;
/// The only major version IceGate emulates.
const EMULATED_MAJOR_VERSION: u32 = 3;
/// Default longest scroll a system-index search may open, in seconds.
const DEFAULT_MAX_SCROLL_LIFETIME_SECS: u64 = 900;
/// Default bound of one tenant's decoded snapshot: four `_bulk` bodies at the default
/// [`DEFAULT_MAX_REQUEST_BODY_BYTES`] fit in it.
const DEFAULT_MAX_SNAPSHOT_BYTES: usize = 4 * DEFAULT_MAX_REQUEST_BODY_BYTES;
/// Default bound of the decoded-snapshot cache: two snapshots at the default bound.
const DEFAULT_SYSTEM_CACHE_MAX_BYTES: usize = 2 * DEFAULT_MAX_SNAPSHOT_BYTES;
/// Default largest `from + size` of a `_search`.
const DEFAULT_MAX_RESULT_WINDOW: usize = 10_000;
/// Default bound of one answer: an answer naming every stored document once fits, with room for
/// the metadata of each entry.
const DEFAULT_MAX_ANSWER_BYTES: usize = 2 * DEFAULT_MAX_SNAPSHOT_BYTES;

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
    /// Bytes of memory one request of this listener may hold: each request's session gets its
    /// own pool of this size, which every plan the session runs (concurrent ones included)
    /// draws from; spilling to temporary files stays enabled, and a query that still needs
    /// more is refused with a 400 naming this limit. Sessions of the other protocols are
    /// unbounded. Validated non-zero while the listener is enabled.
    pub max_query_memory_bytes: usize,
    /// How far back, in seconds, field discovery (`_field_caps`, `_mapping`) looks for
    /// attribute-map keys. A key seen only before the window, or only in the WAL, is not listed;
    /// a path naming it still resolves.
    pub field_discovery_window_secs: u64,
    /// How far, in seconds, before the earliest start of the spans it decorates the root span
    /// of a trace is looked for (`traceGroup` and `traceGroupFields` of `otel-v1-apm-span`). A
    /// root that started earlier renders those fields absent. Validated non-zero while the
    /// listener is enabled.
    pub max_trace_lookback_secs: u64,
    /// The most traces whose root spans one search reads for a filter, sort or aggregation on
    /// `traceGroup` or `traceGroupFields`, and the largest `terms` size on `traceId` of
    /// `otel-v1-apm-span`. Above it the search is a 400: IceGate never truncates the set.
    /// Validated non-zero while the listener is enabled.
    pub max_cross_row_traces: usize,
    /// The per-tenant store of dot-prefixed system indices.
    pub system_indices: SystemIndicesConfig,
    /// Search limits (`search.max_result_window`, `search.max_buckets`).
    pub search: SearchLimitsConfig,
    /// Limits of the PPL endpoint.
    pub ppl: PplLimitsConfig,
}

impl OpenSearchConfig {
    /// Validate the section. Nothing is checked while it is disabled, so a deployment that never
    /// enables it cannot fail on it.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Config`] for a blank host or cluster name, a zero
    /// `max_request_body_bytes`, `max_query_memory_bytes`, `field_discovery_window_secs`,
    /// `max_trace_lookback_secs` or `max_cross_row_traces`, or
    /// a [`version`](Self::version) outside its documented form, and every error of
    /// [`SearchLimitsConfig::validate`], [`PplLimitsConfig::validate`] and
    /// [`SystemIndicesConfig::validate`].
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
        if self.max_query_memory_bytes == 0 {
            return Err(QueryError::Config(
                "OpenSearch max_query_memory_bytes must be greater than zero".into(),
            ));
        }
        if self.field_discovery_window_secs == 0 {
            return Err(QueryError::Config(
                "OpenSearch field_discovery_window_secs must be greater than zero".into(),
            ));
        }
        if self.max_trace_lookback_secs == 0 {
            return Err(QueryError::Config(
                "OpenSearch max_trace_lookback_secs must be greater than zero".into(),
            ));
        }
        if self.max_cross_row_traces == 0 {
            return Err(QueryError::Config(
                "OpenSearch max_cross_row_traces must be greater than zero".into(),
            ));
        }
        EmulatedVersion::parse(&self.version)?;
        self.search.validate()?;
        self.ppl.validate()?;
        self.system_indices.validate()
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
            max_query_memory_bytes: DEFAULT_MAX_QUERY_MEMORY_BYTES,
            field_discovery_window_secs: DEFAULT_FIELD_DISCOVERY_WINDOW_SECS,
            max_trace_lookback_secs: DEFAULT_MAX_TRACE_LOOKBACK_SECS,
            max_cross_row_traces: DEFAULT_MAX_CROSS_ROW_TRACES,
            system_indices: SystemIndicesConfig::default(),
            search: SearchLimitsConfig::default(),
            ppl: PplLimitsConfig::default(),
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

/// Default of [`SearchLimitsConfig::max_buckets`].
const DEFAULT_MAX_BUCKETS: usize = 65_535;

/// Limits of `_search` (`opensearch.search`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchLimitsConfig {
    /// Largest `from + size` a `_search` accepts, on data and on system indices alike, and the
    /// largest page a system-index scroll opens with. A larger window is answered with a 400
    /// `search_phase_execution_exception`, never truncated.
    pub max_result_window: usize,
    /// The most buckets one search's aggregations may answer, every level of every aggregation
    /// counted. A search that would answer more is refused with `too_many_buckets_exception`
    /// (`400`), never truncated.
    pub max_buckets: usize,
}

impl Default for SearchLimitsConfig {
    fn default() -> Self {
        Self {
            max_result_window: DEFAULT_MAX_RESULT_WINDOW,
            max_buckets: DEFAULT_MAX_BUCKETS,
        }
    }
}

impl SearchLimitsConfig {
    /// Check the limits.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Config`] when `max_result_window` or `max_buckets` is zero.
    pub fn validate(&self) -> Result<()> {
        if self.max_result_window == 0 {
            return Err(QueryError::Config(
                "opensearch.search.max_result_window must be greater than 0".to_string(),
            ));
        }
        if self.max_buckets == 0 {
            return Err(QueryError::Config(
                "opensearch.search.max_buckets must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Limits of `/_plugins/_ppl` (`opensearch.ppl`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PplLimitsConfig {
    /// The most rows a PPL query may return, counted after the request's `fetch_size`. A query
    /// whose output has more rows is refused with a 400 naming this limit; it is never
    /// truncated. `None` (the default) sets no row limit: the query deadline, the per-query
    /// memory limit (which counts the returned rows and the answer rendered from them) and load
    /// shedding still bound every query.
    pub max_result_rows: Option<usize>,
}

impl PplLimitsConfig {
    /// Validate the limits.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Config`] for a row limit of 0, which would refuse every query.
    pub fn validate(&self) -> Result<()> {
        if self.max_result_rows == Some(0) {
            return Err(QueryError::Config(
                "opensearch.ppl.max_result_rows must be at least 1 when set".to_string(),
            ));
        }
        Ok(())
    }
}

/// Where and within which bounds the per-tenant system-index store keeps saved objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SystemIndicesConfig {
    /// `s3://` or `s3a://` URL the store writes under. `None` resolves to
    /// [`DEFAULT_LOCATION_PREFIX`](Self::DEFAULT_LOCATION_PREFIX) under the storage bucket (the
    /// location [`build_system_object_store`](crate::opensearch::build_system_object_store)
    /// resolves and [`SystemObjectStore::location`](crate::opensearch::SystemObjectStore::location)
    /// reports), and is an error when the storage backend is not S3. It must not overlap a table
    /// location or the WAL base path.
    pub location: Option<String>,
    /// Longest `scroll=` a system-index search may open, in seconds. Garbage collection keeps a
    /// superseded snapshot object this long plus the query deadline after its successor was
    /// written, so a scroll never loses the snapshot it reads.
    pub max_scroll_lifetime_secs: u64,
    /// Bound of the decoded-snapshot cache, in bytes; at least `max_snapshot_bytes`.
    pub cache_max_bytes: usize,
    /// Largest decoded (uncompressed JSON) snapshot one tenant may hold, in bytes. A write that
    /// would exceed it is refused with `413` and changes nothing.
    pub max_snapshot_bytes: usize,
    /// The bytes of stored documents one answer may carry; at least `max_snapshot_bytes`, so an
    /// answer naming each stored document once fits. An `_mget` or a `_search` past it is refused
    /// whole with `400`, `_bulk` update items past it fail one by one, and the answered items of an
    /// `_msearch` share it: an item past what the earlier ones left fails alone.
    pub max_answer_bytes: usize,
}

impl SystemIndicesConfig {
    /// The key prefix, under the storage bucket, that a `None` [`location`](Self::location)
    /// resolves to.
    pub const DEFAULT_LOCATION_PREFIX: &str = "opensearch-system";

    /// Validate the bounds and the form of the location.
    ///
    /// Whether the location overlaps another owner's objects is checked where each input is
    /// known: `QueryConfig::validate` checks the WAL base path, and the listener's startup checks
    /// the table locations it lists through the catalog.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Config`] for a zero bound, a cache or an answer bound smaller than one
    /// snapshot, or a location that is not an `s3://` or `s3a://` URL naming a bucket.
    pub fn validate(&self) -> Result<()> {
        let zero_bound = [
            ("max_scroll_lifetime_secs", self.max_scroll_lifetime_secs == 0),
            ("cache_max_bytes", self.cache_max_bytes == 0),
            ("max_snapshot_bytes", self.max_snapshot_bytes == 0),
            ("max_answer_bytes", self.max_answer_bytes == 0),
        ]
        .into_iter()
        .find_map(|(key, is_zero)| is_zero.then_some(key));
        if let Some(key) = zero_bound {
            return Err(QueryError::Config(format!(
                "opensearch.system_indices.{key} must be greater than zero"
            )));
        }
        if self.cache_max_bytes < self.max_snapshot_bytes {
            return Err(QueryError::Config(format!(
                "opensearch.system_indices.cache_max_bytes ({}) must be at least max_snapshot_bytes ({}): \
                 the cache holds whole decoded snapshots",
                self.cache_max_bytes, self.max_snapshot_bytes
            )));
        }
        if self.max_answer_bytes < self.max_snapshot_bytes {
            return Err(QueryError::Config(format!(
                "opensearch.system_indices.max_answer_bytes ({}) must be at least max_snapshot_bytes ({}): \
                 an answer naming each stored document once has to fit",
                self.max_answer_bytes, self.max_snapshot_bytes
            )));
        }
        if let Some(location) = &self.location {
            parse_s3_location(location)?;
        }
        Ok(())
    }
}

impl Default for SystemIndicesConfig {
    fn default() -> Self {
        Self {
            location: None,
            max_scroll_lifetime_secs: DEFAULT_MAX_SCROLL_LIFETIME_SECS,
            cache_max_bytes: DEFAULT_SYSTEM_CACHE_MAX_BYTES,
            max_snapshot_bytes: DEFAULT_MAX_SNAPSHOT_BYTES,
            max_answer_bytes: DEFAULT_MAX_ANSWER_BYTES,
        }
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
            max_query_memory_bytes: 1,
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
            "max_query_memory_bytes": 1_073_741_825,
            "field_discovery_window_secs": 3601,
            "max_trace_lookback_secs": 3601,
            "max_cross_row_traces": 1001,
            "system_indices": {
                "location": "s3://saved-objects/tenants",
                "max_scroll_lifetime_secs": 901,
                "cache_max_bytes": 268_435_457,
                "max_snapshot_bytes": 134_217_729,
                "max_answer_bytes": 268_435_458,
            },
            "search": {
                "max_result_window": 7,
                "max_buckets": 9,
            },
            "ppl": {
                "max_result_rows": 11,
            },
        }))
        .expect("a section naming every key deserializes");

        assert!(config.enabled);
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 9201);
        assert_eq!(config.version, "3.10.1");
        assert_eq!(config.cluster_name, "c1");
        assert_eq!(config.max_request_body_bytes, 1_048_577);
        assert_eq!(config.max_query_memory_bytes, 1_073_741_825);
        assert_eq!(config.field_discovery_window_secs, 3601);
        assert_eq!(config.max_trace_lookback_secs, 3601);
        assert_eq!(config.max_cross_row_traces, 1001);
        assert_eq!(
            config.system_indices.location.as_deref(),
            Some("s3://saved-objects/tenants")
        );
        assert_eq!(config.system_indices.max_scroll_lifetime_secs, 901);
        assert_eq!(config.system_indices.cache_max_bytes, 268_435_457);
        assert_eq!(config.system_indices.max_snapshot_bytes, 134_217_729);
        assert_eq!(config.system_indices.max_answer_bytes, 268_435_458);
        assert_eq!(config.search.max_result_window, 7);
        assert_eq!(config.search.max_buckets, 9);
        assert_eq!(config.ppl.max_result_rows, Some(11));
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
        assert_eq!(config.system_indices.location, None);
        assert_eq!(config.system_indices.max_scroll_lifetime_secs, 900);
        assert_eq!(config.system_indices.cache_max_bytes, 268_435_456);
        assert_eq!(config.system_indices.max_snapshot_bytes, 134_217_728);
        assert_eq!(config.system_indices.max_answer_bytes, 268_435_456);
    }

    /// The size of one maximum saved-object import; IceGate sizes the snapshot bound for two such
    /// imports plus one reindexed copy of both.
    const IMPORT_BOUND_BYTES: usize = 26_214_400;

    #[test]
    fn system_indices_defaults_hold_two_maximum_imports_and_their_reindex_copy() {
        let defaults = SystemIndicesConfig::default();

        assert_eq!(defaults.location, None);
        assert_eq!(defaults.max_scroll_lifetime_secs, 900);
        assert_eq!(defaults.max_snapshot_bytes, 134_217_728);
        assert_eq!(defaults.cache_max_bytes, 268_435_456);
        assert_eq!(defaults.max_answer_bytes, 268_435_456);
        assert!(defaults.max_snapshot_bytes >= 4 * IMPORT_BOUND_BYTES);
        assert!(defaults.cache_max_bytes >= defaults.max_snapshot_bytes);
        assert!(defaults.validate().is_ok());
    }

    /// The store keeps its objects where every replica reaches them with conditional create,
    /// compare-and-swap and conditional GET, so only an S3 URL naming a bucket is accepted. A
    /// leading `//` names an empty first segment, which a path parser would strip and so address
    /// another prefix than the one the overlap rule compares.
    #[test]
    fn system_indices_validation_refuses_a_non_s3_location() {
        for location in [
            "/var/data",
            "file:///x",
            "memory://bucket",
            "s3://",
            "s3:///prefix",
            "s3://bucket/a//b",
            "s3://bucket//prefix",
        ] {
            let config = SystemIndicesConfig {
                location: Some(location.to_string()),
                ..SystemIndicesConfig::default()
            };

            assert!(
                matches!(config.validate(), Err(QueryError::Config(_))),
                "{location:?} must be refused"
            );
        }
        for location in [
            "s3://bucket",
            "s3://bucket/opensearch-system",
            "s3a://bucket/nested/prefix/",
        ] {
            let config = SystemIndicesConfig {
                location: Some(location.to_string()),
                ..SystemIndicesConfig::default()
            };

            assert!(config.validate().is_ok(), "{location:?} must be accepted");
        }
    }

    #[test]
    fn system_indices_validation_refuses_zero_limits() {
        let cases: [(&str, fn(&mut SystemIndicesConfig)); 4] = [
            ("max_scroll_lifetime_secs", |config| config.max_scroll_lifetime_secs = 0),
            ("cache_max_bytes", |config| config.cache_max_bytes = 0),
            ("max_snapshot_bytes", |config| config.max_snapshot_bytes = 0),
            ("max_answer_bytes", |config| config.max_answer_bytes = 0),
        ];
        for (key, zero) in cases {
            let mut config = SystemIndicesConfig::default();
            zero(&mut config);

            assert!(
                matches!(config.validate(), Err(QueryError::Config(message)) if message.contains(key)),
                "{key}"
            );
        }
    }

    /// The zero refusal stops at zero: the smallest non-zero bounds are accepted.
    #[test]
    fn system_indices_validation_accepts_limits_of_one() {
        let config = SystemIndicesConfig {
            location: None,
            max_scroll_lifetime_secs: 1,
            cache_max_bytes: 1,
            max_snapshot_bytes: 1,
            max_answer_bytes: 1,
        };

        assert!(config.validate().is_ok());
    }

    /// The cache holds whole decoded snapshots, so a bound below one snapshot could never hold the
    /// largest one; equal is the smallest accepted bound.
    #[test]
    fn system_indices_validation_refuses_a_cache_smaller_than_one_snapshot() {
        let undersized_cache_config = SystemIndicesConfig {
            cache_max_bytes: 1_000_000,
            max_snapshot_bytes: 1_000_001,
            ..SystemIndicesConfig::default()
        };
        let equal_cache_config = SystemIndicesConfig {
            cache_max_bytes: 1_000_000,
            max_snapshot_bytes: 1_000_000,
            ..SystemIndicesConfig::default()
        };

        assert!(matches!(undersized_cache_config.validate(), Err(QueryError::Config(_))));
        assert!(equal_cache_config.validate().is_ok());
    }

    /// An answer naming each stored document once has to fit, so a bound below one snapshot could
    /// refuse a request no client can make smaller; equal is the smallest accepted bound.
    #[test]
    fn system_indices_validation_refuses_an_answer_bound_smaller_than_one_snapshot() {
        let undersized_answer_config = SystemIndicesConfig {
            max_answer_bytes: 1_000_000,
            max_snapshot_bytes: 1_000_001,
            ..SystemIndicesConfig::default()
        };
        let equal_answer_config = SystemIndicesConfig {
            max_answer_bytes: 1_000_000,
            max_snapshot_bytes: 1_000_000,
            ..SystemIndicesConfig::default()
        };

        assert!(matches!(
            undersized_answer_config.validate(),
            Err(QueryError::Config(message))
                if message.contains("max_answer_bytes") && message.contains("max_snapshot_bytes")
        ));
        assert!(equal_answer_config.validate().is_ok());
    }

    #[test]
    fn an_enabled_section_validates_its_system_indices() {
        let config = OpenSearchConfig {
            enabled: true,
            system_indices: SystemIndicesConfig {
                max_snapshot_bytes: 0,
                ..SystemIndicesConfig::default()
            },
            ..OpenSearchConfig::default()
        };

        assert!(matches!(config.validate(), Err(QueryError::Config(_))));
    }

    #[test]
    fn search_limits_default_to_a_window_of_ten_thousand_and_refuse_zero() {
        assert_eq!(OpenSearchConfig::default().search.max_result_window, 10_000);
        let mut config = OpenSearchConfig {
            enabled: true,
            ..OpenSearchConfig::default()
        };
        config.search.max_result_window = 0;
        assert!(matches!(config.validate(), Err(QueryError::Config(_))));
        config.search.max_result_window = 1;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn search_limits_default_to_65535_buckets_and_refuse_zero() {
        assert_eq!(OpenSearchConfig::default().search.max_buckets, 65_535);

        let mut config = OpenSearchConfig {
            enabled: true,
            ..OpenSearchConfig::default()
        };
        config.search.max_buckets = 0;
        assert!(matches!(config.validate(), Err(QueryError::Config(_))));
        config.search.max_buckets = 1;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn the_query_memory_limit_defaults_to_one_gibibyte() {
        assert_eq!(
            OpenSearchConfig::default().max_query_memory_bytes,
            DEFAULT_MAX_QUERY_MEMORY_BYTES
        );
        assert_eq!(DEFAULT_MAX_QUERY_MEMORY_BYTES, 1_073_741_824);
    }

    #[test]
    fn validate_rejects_a_zero_query_memory_limit_when_enabled() {
        let config = OpenSearchConfig {
            enabled: true,
            max_query_memory_bytes: 0,
            ..OpenSearchConfig::default()
        };
        assert!(matches!(config.validate(), Err(QueryError::Config(_))));

        let disabled = OpenSearchConfig {
            enabled: false,
            max_query_memory_bytes: 0,
            ..OpenSearchConfig::default()
        };
        disabled.validate().expect("a disabled listener validates nothing");
    }

    #[test]
    fn ppl_limits_default_to_no_row_limit_and_refuse_a_zero_limit() {
        let config = OpenSearchConfig::default();
        assert_eq!(config.ppl, PplLimitsConfig { max_result_rows: None });
        assert!(config.ppl.validate().is_ok());
        assert!(
            PplLimitsConfig {
                max_result_rows: Some(1)
            }
            .validate()
            .is_ok()
        );
        let zero_limit_config = OpenSearchConfig {
            enabled: true,
            ppl: PplLimitsConfig {
                max_result_rows: Some(0),
            },
            ..OpenSearchConfig::default()
        };
        assert!(matches!(zero_limit_config.validate(), Err(QueryError::Config(_))));
        let limited_config: OpenSearchConfig =
            serde_json::from_value(json!({"ppl": {"max_result_rows": 5000}})).expect("ppl section");
        assert_eq!(limited_config.ppl.max_result_rows, Some(5000));
        let null_limit_config: OpenSearchConfig =
            serde_json::from_value(json!({"ppl": {"max_result_rows": null}})).expect("null limit");
        assert_eq!(null_limit_config.ppl.max_result_rows, None);
    }

    #[test]
    fn cross_row_limits_default_to_an_hour_and_a_thousand_traces_and_refuse_zero() {
        let defaults = OpenSearchConfig::default();
        assert_eq!(defaults.max_trace_lookback_secs, 3_600);
        assert_eq!(defaults.max_cross_row_traces, 1_000);

        let enabled = build_enabled_config();
        assert!(enabled.validate().is_ok());
        let no_lookback = OpenSearchConfig {
            max_trace_lookback_secs: 0,
            ..enabled.clone()
        };
        let error = no_lookback.validate().expect_err("a zero lookback is refused");
        assert!(matches!(error, QueryError::Config(_)), "{error:?}");
        let no_traces = OpenSearchConfig {
            max_cross_row_traces: 0,
            ..enabled
        };
        let error = no_traces.validate().expect_err("a zero trace bound is refused");
        assert!(matches!(error, QueryError::Config(_)), "{error:?}");
        assert!(
            OpenSearchConfig {
                max_trace_lookback_secs: 0,
                max_cross_row_traces: 0,
                ..OpenSearchConfig::default()
            }
            .validate()
            .is_ok(),
            "nothing is checked while disabled"
        );
    }
}
