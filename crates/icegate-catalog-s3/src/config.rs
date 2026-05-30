//! Configuration for the S3-backed catalog.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crate::codec::{CatalogCodec, json::JsonCatalogCodec};
use crate::error::{Error, Result};
use crate::infra::retrier::RetrierConfig;

/// Build the backoff curve for transient storage I/O (timeouts, throttling, 5xx).
///
/// Few attempts with a capped tail: a transient fault either clears within a
/// handful of growing backoffs or is escalated to the caller. With `max_attempts`
/// = 7 the loop sleeps 6 times over the `[50, 100, 200, 500]` curve (the last
/// delay clamps), so the whole budget is bounded at roughly 2 seconds (worst
/// case: 50+100+200+500+500+500 ms base plus ≤50 ms jitter each). A hard failure
/// misclassified as transient — e.g. bad credentials surfaced by `object_store`
/// as `Generic` — therefore stalls the hot ingest path for seconds, not minutes.
/// See `error.rs` for that mapping.
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

impl std::str::FromStr for CatalogCodecKind {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "json" => Ok(Self::Json),
            other => Err(Error::InvalidMetadata(format!("Unsupported catalog codec: {other}"))),
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

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::infra::retrier::{Retrier, RetryError};

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
