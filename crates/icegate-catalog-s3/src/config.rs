//! Configuration for the S3-backed catalog.

#[cfg(feature = "rest")]
use std::collections::HashMap;
#[cfg(feature = "rest")]
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "rest")]
use iceberg::io::{FileIO, FileIOBuilder};
#[cfg(feature = "rest")]
use iceberg_storage_opendal::OpenDalStorageFactory;
#[cfg(feature = "rest")]
use serde::Deserialize;

#[cfg(feature = "rest")]
use crate::api::{CatalogApiConfig, CatalogApiConfigError, CatalogPrefix};
use crate::infra::retrier::RetrierConfig;
use crate::storage::codec::{CatalogCodec, json::JsonCatalogCodec};

/// Build the backoff curve for transient storage I/O (timeouts, throttling, 5xx).
///
/// Few attempts with a capped tail: a transient fault either clears within a
/// handful of growing backoffs or is escalated to the caller. With `max_attempts`
/// = 7 the loop sleeps 6 times over the `[50, 100, 200, 500]` curve (the last
/// delay clamps), so the whole budget is bounded at roughly 2 seconds (worst
/// case: 50+100+200+500+500+500 ms base plus ≤50 ms jitter each). Permanent auth
/// failures (403/401) are classified non-retryable and fail fast; only the rare
/// permanent case hidden inside `Generic` (e.g. 400 or a wrong endpoint) inherits
/// this curve, so it stalls the hot ingest path for seconds, not minutes. See
/// `error.rs` for that mapping.
pub(crate) fn storage_retrier_config_default() -> RetrierConfig {
    RetrierConfig {
        max_attempts: 7,
        rand_delay: Duration::from_millis(50),
        delays: vec![
            Duration::from_millis(50),
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(500),
        ],
    }
}

/// Build the backoff curve for optimistic CAS-conflict loops on the catalog root.
///
/// A CAS conflict means another writer already committed; the loser only has to
/// reload, re-merge, and re-publish — work that completes in milliseconds. So
/// this curve favours many cheap, fast attempts with a small cap (1 second) and
/// a jitter that dominates the early base delays, decorrelating concurrent
/// writers to avoid a thundering herd. The whole budget is bounded at roughly
/// 16 seconds (worst case) — seconds, not the minutes a long I/O tail would
/// cost on a sustained conflict.
pub(crate) fn cas_retrier_config_default() -> RetrierConfig {
    RetrierConfig {
        max_attempts: 20,
        rand_delay: Duration::from_millis(100),
        delays: vec![
            // Index 0 is the base delay of the first retry (attempt 1 reads
            // `delays[0]`); 0 here means that retry waits on jitter alone.
            Duration::from_millis(0),
            Duration::from_millis(10),
            Duration::from_millis(25),
            Duration::from_millis(50),
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(800),
            Duration::from_secs(1),
        ],
    }
}

/// Default cap for the in-memory table-metadata cache (entry count).
///
/// Sized to hold the metadata of a healthy working set of active tables. On
/// append-heavy ingest each commit mints a fresh UUID-suffixed location that is
/// read once, so the cache barely hits there and the cap exists purely to bound
/// memory; query workloads that re-read the same tables get the real benefit.
const DEFAULT_METADATA_CACHE_CAP: NonZeroUsize = match NonZeroUsize::new(8) {
    Some(cap) => cap,
    // value is non-zero; the fallback keeps this a compile-time const with no
    // `unwrap`/`expect`/`panic!` (forbidden in non-test code).
    None => NonZeroUsize::MIN,
};

/// Decide whether S3 requests address the bucket through the path rather than
/// the host.
///
/// A `configured` value is the operator's decision and always wins. Without one
/// the endpoint decides, because that is the only thing that distinguishes the
/// two deployments: an S3-compatible endpoint (RustFS, `MinIO`) serves a bucket
/// only path-style, while AWS itself has deprecated path-style and answers
/// virtual-hosted.
#[must_use]
pub const fn resolve_path_style_access(configured: Option<bool>, endpoint: Option<&str>) -> bool {
    match configured {
        Some(path_style_access) => path_style_access,
        None => endpoint.is_some(),
    }
}

/// Format the warehouse URI `s3://{bucket}[/{warehouse}]`.
///
/// The one place the URI shape is defined: the warehouse announced over REST,
/// the catalog's table-location prefix, and the table `FileIO` warehouse
/// property are all derived from it, so they cannot silently drift apart.
///
/// Pure and unvalidated: callers own the check that `bucket` names a bucket.
#[must_use]
pub fn format_warehouse_uri(bucket: &str, warehouse: &str) -> String {
    let warehouse = warehouse.trim_matches('/');
    if warehouse.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{warehouse}")
    }
}

/// Catalog root serialization codec kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CatalogCodecKind {
    /// JSON serialization.
    #[default]
    Json,
}

impl CatalogCodecKind {
    pub(crate) fn into_codec(self) -> Arc<dyn CatalogCodec> {
        match self {
            Self::Json => Arc::new(JsonCatalogCodec),
        }
    }
}

/// Error returned when a codec name does not identify a supported catalog codec.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("unsupported catalog codec: {0}")]
pub struct UnsupportedCatalogCodecError(String);

impl std::str::FromStr for CatalogCodecKind {
    type Err = UnsupportedCatalogCodecError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "json" => Ok(Self::Json),
            other => Err(UnsupportedCatalogCodecError(other.to_string())),
        }
    }
}

/// S3 catalog configuration.
#[derive(Debug, Clone)]
pub struct S3CatalogConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// AWS region.
    pub region: String,
    /// Optional S3-compatible endpoint.
    pub endpoint: Option<String>,
    /// Whether S3 requests address the bucket through the path rather than the host,
    /// or `None` to let [`resolve_path_style_access`] derive it from the endpoint.
    ///
    /// This is shared by catalog-root storage and the `FileIO` installed in returned
    /// tables, so metadata and data always use the same addressing policy. That is
    /// also why the resolved value overrides `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`,
    /// which `object_store` would otherwise read from the environment: honouring it
    /// on the catalog-root side alone would split the two apart again.
    pub path_style_access: Option<bool>,
    /// Optional explicit S3 access key (mainly for local test environments).
    ///
    /// When omitted, credentials are resolved from the environment and default AWS provider chain.
    pub access_key_id: Option<String>,
    /// Optional explicit S3 secret key (mainly for local test environments).
    ///
    /// When omitted, credentials are resolved from the environment and default AWS provider chain.
    pub secret_access_key: Option<String>,
    /// S3 key prefix used as warehouse root (without leading/trailing `/`).
    pub warehouse: String,
    /// Serialization codec for catalog root only.
    pub codec: CatalogCodecKind,
    /// Retry policy for transient storage I/O (timeouts, throttling, 5xx).
    ///
    /// Drives the storage layer's backoff. Defaults to
    /// [`storage_retrier_config_default`]: a short, capped tail so a misclassified
    /// hard failure stalls the ingest path for seconds, not minutes.
    pub storage_retrier_config: RetrierConfig,
    /// Retry policy for optimistic CAS-conflict loops on the catalog root.
    ///
    /// Drives the catalog layer's commit/update retries. Defaults to
    /// [`cas_retrier_config_default`]: many cheap, fast attempts with jitter,
    /// because a CAS conflict resolves in milliseconds and must not inherit the
    /// long I/O backoff tail.
    pub cas_retrier_config: RetrierConfig,
    /// Enable the in-memory caching decorator over storage (default `true`).
    ///
    /// Disable in tests that need to observe raw storage round-trips.
    pub enable_cache: bool,
    /// Maximum number of table-metadata entries kept in the in-memory cache.
    ///
    /// Metadata files are immutable (UUID-suffixed locations), so entries are
    /// evicted by LRU rather than invalidated. The cap bounds memory on a
    /// long-lived ingest process, where every commit mints a fresh location
    /// read exactly once. Only consulted when [`Self::enable_cache`] is set.
    pub metadata_cache_cap: NonZeroUsize,
}

impl Default for S3CatalogConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: String::new(),
            endpoint: None,
            path_style_access: None,
            access_key_id: None,
            secret_access_key: None,
            warehouse: String::new(),
            codec: CatalogCodecKind::default(),
            storage_retrier_config: storage_retrier_config_default(),
            cas_retrier_config: cas_retrier_config_default(),
            enable_cache: true,
            metadata_cache_cap: DEFAULT_METADATA_CACHE_CAP,
        }
    }
}

/// Standalone `CatalogServer` process configuration.
#[cfg(feature = "rest")]
#[derive(Debug, Clone, Deserialize)]
pub struct CatalogServerConfig {
    /// HTTP listener and pagination settings.
    pub http: CatalogServerHttpConfig,
    /// S3 storage settings shared by catalog root and returned table `FileIO`s.
    pub s3: CatalogServerS3Config,
}

/// HTTP settings for the standalone `CatalogServer`.
#[cfg(feature = "rest")]
#[derive(Debug, Clone, Deserialize)]
pub struct CatalogServerHttpConfig {
    /// Listener host.
    pub host: String,
    /// Listener port.
    pub port: u16,
    /// Optional validated exact REST catalog prefix, relative to `/v1`.
    #[serde(default)]
    pub prefix: Option<CatalogPrefix>,
    /// Page size used by a paginated request that omits `pageSize`.
    pub default_page_size: usize,
    /// Maximum number of records returned by a page.
    pub max_page_size: usize,
}

/// S3 settings for the standalone `CatalogServer`.
#[cfg(feature = "rest")]
#[derive(Clone, Deserialize)]
pub struct CatalogServerS3Config {
    /// S3 bucket containing the catalog root and table metadata.
    pub bucket: String,
    /// AWS region.
    pub region: String,
    /// Optional S3-compatible endpoint.
    pub endpoint: Option<String>,
    /// Whether both root storage and table `FileIO` use path-style addressing.
    ///
    /// See [`S3CatalogConfig::path_style_access`] for what an omitted value derives
    /// and what a set one overrides.
    #[serde(default)]
    pub path_style_access: Option<bool>,
    /// Optional explicit access key.
    pub access_key_id: Option<String>,
    /// Optional explicit secret key.
    pub secret_access_key: Option<String>,
    /// S3 warehouse prefix.
    pub warehouse: String,
    /// Catalog root codec name.
    #[serde(default = "default_catalog_codec")]
    pub codec: String,
}

#[cfg(feature = "rest")]
impl std::fmt::Debug for CatalogServerS3Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogServerS3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("path_style_access", &self.path_style_access)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "<redacted>"),
            )
            .field("warehouse", &self.warehouse)
            .field("codec", &self.codec)
            .finish()
    }
}

#[cfg(feature = "rest")]
fn default_catalog_codec() -> String {
    "json".to_string()
}

/// Rejected or unreadable standalone `CatalogServer` process configuration.
///
/// Raised before a listener is bound or S3 is touched, so the `serve` command
/// fails on a configuration error rather than a late runtime failure.
#[cfg(feature = "rest")]
#[derive(Debug, thiserror::Error)]
pub enum CatalogServerConfigError {
    /// The configuration file could not be read.
    #[error("failed to read catalog config {}: {source}", path.display())]
    Read {
        /// Path of the unreadable file.
        path: std::path::PathBuf,
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },

    /// The file extension does not select a supported configuration format.
    #[error("unsupported catalog config extension for {}: expected .yaml, .yml, or .toml", path.display())]
    UnsupportedExtension {
        /// Path whose extension selects no format.
        path: std::path::PathBuf,
    },

    /// The file contents do not deserialize into a server configuration.
    #[error("invalid catalog server config: {0}")]
    Parse(String),

    /// The S3 bucket is empty or contains whitespace.
    #[error("catalog S3 bucket cannot be empty or contain whitespace")]
    InvalidBucket,

    /// The S3 region is empty.
    #[error("catalog S3 region cannot be empty")]
    EmptyRegion,

    /// `access_key_id` is set but `secret_access_key` is missing.
    #[error("catalog S3 access_key_id is set but secret_access_key is missing")]
    AccessKeyWithoutSecretKey,

    /// `secret_access_key` is set but `access_key_id` is missing.
    #[error("catalog S3 secret_access_key is set but access_key_id is missing")]
    SecretKeyWithoutAccessKey,

    /// The codec name does not identify a supported catalog codec.
    #[error(transparent)]
    Codec(#[from] UnsupportedCatalogCodecError),

    /// Host and port do not form a socket address.
    #[error("invalid catalog listen address: {0}")]
    InvalidListenAddress(#[from] std::net::AddrParseError),

    /// The settings cannot form a valid [`CatalogApiConfig`].
    #[error(transparent)]
    Api(#[from] CatalogApiConfigError),
}

#[cfg(feature = "rest")]
impl CatalogServerConfig {
    /// Load and validate `YAML` or `TOML` process configuration from `path`.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self, CatalogServerConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path).map_err(|source| CatalogServerConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let extension = path.extension().and_then(std::ffi::OsStr::to_str).unwrap_or_default();
        let config: Self = match extension {
            "yaml" | "yml" => {
                serde_yaml::from_str(&contents).map_err(|error| CatalogServerConfigError::Parse(error.to_string()))?
            }
            "toml" => toml::from_str(&contents).map_err(|error| CatalogServerConfigError::Parse(error.to_string()))?,
            _ => {
                return Err(CatalogServerConfigError::UnsupportedExtension {
                    path: path.to_path_buf(),
                });
            }
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate process settings before binding a listener or accessing S3.
    ///
    /// REST behaviour is validated by building a [`CatalogApiConfig`], so a
    /// configuration file can never load settings the router would later reject.
    pub fn validate(&self) -> Result<(), CatalogServerConfigError> {
        self.validate_s3()?;
        self.listen_address()?;
        self.to_api_config()?;
        Ok(())
    }

    /// Validate the S3 settings the catalog root, table `FileIO`, and announced
    /// warehouse URI are all built from.
    ///
    /// Split out of [`Self::validate`] because that method validates REST
    /// behaviour by building on the warehouse URI: reaching back into it from
    /// [`Self::warehouse_uri`] would recurse.
    fn validate_s3(&self) -> Result<(), CatalogServerConfigError> {
        self.validate_bucket()?;
        if self.s3.region.trim().is_empty() {
            return Err(CatalogServerConfigError::EmptyRegion);
        }
        match (&self.s3.access_key_id, &self.s3.secret_access_key) {
            (Some(_), None) => return Err(CatalogServerConfigError::AccessKeyWithoutSecretKey),
            (None, Some(_)) => return Err(CatalogServerConfigError::SecretKeyWithoutAccessKey),
            _ => {}
        }
        // The codec is converted again by `to_s3_catalog_config`; parsing it
        // here keeps `validate` the single point that admits a file, so an
        // unsupported codec cannot load and only fail on a later conversion.
        self.s3.codec.parse::<CatalogCodecKind>()?;
        Ok(())
    }

    /// Check that `s3.bucket` is non-empty and free of whitespace.
    ///
    /// That is the whole invariant, and deliberately not a bucket-name syntax
    /// check: the storage layer owns the full S3 naming rules, and a partial
    /// copy of them here would only drift. What this guards is that one value is
    /// usable verbatim in all three of its destinations — the warehouse URI
    /// announced over REST, the [`S3CatalogConfig`] the storage layer addresses,
    /// and the table [`FileIO`]. Whitespace is therefore rejected rather than
    /// trimmed: normalizing it for one consumer would leave the others
    /// addressing a different bucket and defer the failure to the first S3 call.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogServerConfigError::InvalidBucket`] when the bucket is
    /// empty or contains whitespace.
    fn validate_bucket(&self) -> Result<(), CatalogServerConfigError> {
        if self.s3.bucket.is_empty() || self.s3.bucket.contains(char::is_whitespace) {
            return Err(CatalogServerConfigError::InvalidBucket);
        }
        Ok(())
    }

    /// Resolve the listener address the `CatalogServer` binds.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogServerConfigError::InvalidListenAddress`] when host and
    /// port do not form a socket address.
    pub fn listen_address(&self) -> Result<SocketAddr, CatalogServerConfigError> {
        let host = &self.http.host;
        let authority = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]:{}", self.http.port) // ipv6
        } else {
            format!("{host}:{}", self.http.port) // ipv4
        };
        Ok(authority.parse()?)
    }

    /// Convert the file configuration into the validated REST API configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the settings cannot form a valid [`CatalogApiConfig`].
    pub fn to_api_config(&self) -> Result<CatalogApiConfig, CatalogServerConfigError> {
        Ok(CatalogApiConfig::new(
            self.warehouse_uri()?,
            self.http.prefix.clone(),
            self.http.default_page_size,
            self.http.max_page_size,
        )?)
    }

    /// Convert the file configuration into the S3 catalog library configuration.
    pub fn to_s3_catalog_config(&self) -> Result<S3CatalogConfig, CatalogServerConfigError> {
        self.validate()?;
        Ok(S3CatalogConfig {
            bucket: self.s3.bucket.clone(),
            region: self.s3.region.clone(),
            endpoint: self.s3.endpoint.clone(),
            path_style_access: self.s3.path_style_access,
            access_key_id: self.s3.access_key_id.clone(),
            secret_access_key: self.s3.secret_access_key.clone(),
            warehouse: self.s3.warehouse.clone(),
            codec: self.s3.codec.parse()?,
            ..S3CatalogConfig::default()
        })
    }

    /// Return the canonical warehouse URI announced by the REST configuration endpoint.
    ///
    /// Fallible because this type is public and its fields are public: a caller
    /// can build one directly and never reach [`Self::from_file`], so the URI
    /// must reject settings that cannot name a warehouse rather than announce
    /// a bare `s3://`.
    ///
    /// # Errors
    ///
    /// Returns an error when [`Self::validate_bucket`] rejects the bucket.
    pub fn warehouse_uri(&self) -> Result<String, CatalogServerConfigError> {
        self.validate_bucket()?;
        Ok(format_warehouse_uri(&self.s3.bucket, &self.s3.warehouse))
    }

    /// Build the production S3 `FileIO` used by tables loaded from this catalog.
    pub fn build_file_io(&self) -> Result<FileIO, CatalogServerConfigError> {
        self.validate()?;
        let mut properties = HashMap::from([
            ("warehouse".to_string(), self.warehouse_uri()?),
            ("s3.region".to_string(), self.s3.region.clone()),
            (
                "s3.path-style-access".to_string(),
                resolve_path_style_access(self.s3.path_style_access, self.s3.endpoint.as_deref()).to_string(),
            ),
        ]);
        if let Some(endpoint) = &self.s3.endpoint {
            properties.insert("s3.endpoint".to_string(), endpoint.clone());
        }
        if let (Some(access_key_id), Some(secret_access_key)) = (&self.s3.access_key_id, &self.s3.secret_access_key) {
            properties.insert("s3.access-key-id".to_string(), access_key_id.clone());
            properties.insert("s3.secret-access-key".to_string(), secret_access_key.clone());
        }
        Ok(FileIOBuilder::new(std::sync::Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        }))
        .with_props(properties)
        .build())
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::infra::retrier::{Retrier, RetryError};

    #[cfg(feature = "rest")]
    #[test]
    fn server_config_deserializes_only_a_validated_catalog_prefix() {
        let valid: CatalogServerConfig = serde_yaml::from_str(
            "\
http:
  host: 127.0.0.1
  port: 8181
  prefix: ice/warehouses/my
  default_page_size: 1
  max_page_size: 2
s3:
  bucket: catalog
  region: us-east-1
  warehouse: catalog
",
        )
        .expect("valid server config");

        assert_eq!(
            valid.http.prefix.as_ref().map(CatalogPrefix::as_str),
            Some("ice/warehouses/my")
        );
        assert!(valid.validate().is_ok());

        for prefix in ["/ice", "ice/", "ice//my", "ice/./my", "ice/../my"] {
            let invalid = serde_yaml::from_str::<CatalogServerConfig>(&format!(
                "\
http:
  host: 127.0.0.1
  port: 8181
  prefix: \"{prefix}\"
  default_page_size: 1
  max_page_size: 2
s3:
  bucket: catalog
  region: us-east-1
  warehouse: catalog
",
            ));

            assert!(invalid.is_err(), "prefix {prefix:?}");
        }
    }

    #[cfg(feature = "rest")]
    fn server_config_with_bucket(bucket: &str) -> CatalogServerConfig {
        CatalogServerConfig {
            http: CatalogServerHttpConfig {
                host: "127.0.0.1".to_string(),
                port: 8181,
                prefix: None,
                default_page_size: 1,
                max_page_size: 2,
            },
            s3: CatalogServerS3Config {
                bucket: bucket.to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                path_style_access: None,
                access_key_id: None,
                secret_access_key: None,
                warehouse: "/nested/prefix/".to_string(),
                codec: "json".to_string(),
            },
        }
    }

    /// An unusable bucket reaches these entry points only through direct
    /// construction: the type and its fields are public, so `from_file`'s
    /// `validate` is not on every path in. Whitespace must be rejected rather
    /// than trimmed by whichever consumer happens to look first — a bucket
    /// silently normalized for the announced warehouse URI but passed verbatim
    /// to storage would leave REST and S3 addressing different buckets. Interior
    /// whitespace has no edge to trim at all, so nothing would catch it before
    /// the first S3 call.
    #[cfg(feature = "rest")]
    #[test]
    fn a_bucket_that_is_empty_or_carries_whitespace_is_rejected_by_every_entry_point() {
        for bucket in [
            "",
            "   ",
            " catalog ",
            "catalog ",
            " catalog",
            "\tcatalog",
            "cat alog",
            "cat\talog",
        ] {
            let config = server_config_with_bucket(bucket);

            assert!(
                matches!(config.warehouse_uri(), Err(CatalogServerConfigError::InvalidBucket)),
                "warehouse_uri accepted bucket {bucket:?}"
            );
            assert!(
                matches!(config.to_api_config(), Err(CatalogServerConfigError::InvalidBucket)),
                "to_api_config accepted bucket {bucket:?}"
            );
            assert!(
                matches!(
                    config.to_s3_catalog_config(),
                    Err(CatalogServerConfigError::InvalidBucket)
                ),
                "to_s3_catalog_config accepted bucket {bucket:?}"
            );
            assert!(
                matches!(config.validate(), Err(CatalogServerConfigError::InvalidBucket)),
                "validate accepted bucket {bucket:?}"
            );
        }
    }

    /// The warehouse URI and the storage config must name one bucket: a client
    /// told to address `s3://catalog` and a storage layer reading some other
    /// bucket is a fault that would only surface on the first S3 call.
    #[cfg(feature = "rest")]
    #[test]
    fn an_accepted_bucket_reaches_the_warehouse_uri_and_the_storage_config_unchanged() {
        let config = server_config_with_bucket("catalog");

        assert_eq!(
            config.warehouse_uri().expect("warehouse uri"),
            "s3://catalog/nested/prefix"
        );
        assert_eq!(
            config.to_s3_catalog_config().expect("s3 catalog config").bucket,
            "catalog"
        );
    }

    #[cfg(feature = "rest")]
    #[test]
    fn warehouse_uri_names_the_bucket_root_when_no_warehouse_prefix_is_configured() {
        let mut config: CatalogServerConfig = serde_yaml::from_str(
            "\
http:
  host: 127.0.0.1
  port: 8181
  default_page_size: 1
  max_page_size: 2
s3:
  bucket: catalog
  region: us-east-1
  warehouse: /nested/prefix/
",
        )
        .expect("valid server config");

        assert_eq!(
            config.warehouse_uri().expect("warehouse uri"),
            "s3://catalog/nested/prefix"
        );

        config.s3.warehouse = "/".to_string();
        assert_eq!(config.warehouse_uri().expect("warehouse uri"), "s3://catalog");
    }

    /// The host names the address family the server listens on, so every form a
    /// `SocketAddr` can hold must reach the listener — including the bare `::`
    /// that requests a dual-stack listener and would parse as neither host nor
    /// port if its brackets were left to the operator.
    #[cfg(feature = "rest")]
    #[test]
    fn listen_address_accepts_every_host_form_a_socket_address_can_hold() {
        for (host, expected) in [
            ("127.0.0.1", "127.0.0.1:8181"),
            ("0.0.0.0", "0.0.0.0:8181"),
            ("::", "[::]:8181"),
            ("::1", "[::1]:8181"),
            ("[::]", "[::]:8181"),
        ] {
            let mut config = server_config_with_bucket("catalog");
            config.http.host = host.to_string();

            assert_eq!(
                config.listen_address().expect("listen address").to_string(),
                expected,
                "host {host:?}",
            );
        }

        let mut config = server_config_with_bucket("catalog");
        config.http.host = "catalog.internal".to_string();

        assert!(
            matches!(
                config.listen_address(),
                Err(CatalogServerConfigError::InvalidListenAddress(_))
            ),
            "a host that is not an address must not bracket its way into a listener",
        );
    }

    /// Addressing is derived, not defaulted to a constant: AWS deprecated
    /// path-style and answers virtual-hosted, while an S3-compatible endpoint
    /// serves a bucket only path-style. An operator's explicit value is a
    /// decision and outranks both.
    #[test]
    fn path_style_access_is_derived_from_the_endpoint_until_it_is_configured() {
        for (configured, endpoint, expected) in [
            (None, None, false),
            (None, Some("http://127.0.0.1:9000"), true),
            (Some(false), None, false),
            (Some(false), Some("http://127.0.0.1:9000"), false),
            (Some(true), None, true),
            (Some(true), Some("http://127.0.0.1:9000"), true),
        ] {
            assert_eq!(
                resolve_path_style_access(configured, endpoint),
                expected,
                "configured {configured:?}, endpoint {endpoint:?}",
            );
        }
    }

    enum TestError {
        Cancelled,
        MaxAttempts,
    }

    impl RetryError for TestError {
        fn cancelled() -> Self {
            Self::Cancelled
        }

        fn max_attempts() -> Self {
            Self::MaxAttempts
        }
    }

    /// Drive a never-resolving conflict to exhaustion and return the total
    /// backoff measured on the virtual clock. `start_paused` auto-advances time
    /// across each `sleep`, so the elapsed value equals the summed delays
    /// without real waiting — letting us assert the worst-case budget.
    async fn measure_exhaustion(config: RetrierConfig) -> Duration {
        let retrier = Retrier::new(config);
        let token = CancellationToken::new();
        let start = tokio::time::Instant::now();
        let result: std::result::Result<(), TestError> = retrier.retry(|| async { Ok((true, ())) }, &token).await;
        assert!(matches!(result, Err(TestError::MaxAttempts)));
        start.elapsed()
    }

    #[tokio::test(start_paused = true)]
    async fn cas_curve_exhausts_in_seconds() {
        let elapsed = measure_exhaustion(cas_retrier_config_default()).await;
        assert!(
            elapsed <= Duration::from_secs(20),
            "CAS budget {elapsed:?} must stay within seconds, not minutes",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn storage_curve_tail_stays_bounded() {
        let elapsed = measure_exhaustion(storage_retrier_config_default()).await;
        // 6 sleeps over [50, 100, 200, 500] (last delay clamps): base sum is
        // 1850 ms, jitter adds 0..50 ms per sleep (≤300 ms). Lock the documented
        // ~2 s budget so the comment and the curve cannot silently drift apart.
        assert!(
            (Duration::from_millis(1850)..=Duration::from_millis(2200)).contains(&elapsed),
            "storage I/O tail {elapsed:?} must match the documented ~2s budget",
        );
    }
}
