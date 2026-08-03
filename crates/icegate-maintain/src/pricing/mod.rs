//! Background crawler that keeps `icegate.prices` current.
//!
//! One jobmanager job fetches every configured source concurrently, normalises
//! and guards the results, then appends changed rates in a single commit. See
//! `docs/superpowers/specs/2026-07-22-llm-pricing-crawler-design.md`.
//!
//! Crawl order, per [`run_crawl`]:
//! 1. Read the live rate card ([`read::read_live_rates`]).
//! 2. Fetch every source concurrently.
//! 3. Per source: a failed fetch contributes nothing and does not fail the
//!    crawl; drop rows for providers the source does not own; the cardinality
//!    guard discards the source's entire batch on failure, not individual
//!    rows; the row guards run; a source with no live rows to compare against
//!    logs a WARN summary, since both live-comparing guards no-op for it.
//! 4. Fail the crawl if *no* source contributed a row — one source going dark
//!    is tolerated, all of them going dark is an outage, not a quiet market.
//! 5. Drop any candidate repeating a key an earlier one already claimed.
//! 6. Derive `canonical_id` over the pooled candidates.
//! 7. Diff against the live rate card.
//! 8. Append changed rates in a single commit — or nothing, if none changed.

/// Cross-provider model identity derivation.
pub mod canonical;
/// Crawler configuration: interval, sources, guard thresholds.
pub mod config;
/// Fixed-point quantization mapping `f64` rates onto the stored decimal grid.
pub mod decimal;

/// Upstream rate-card sources and the observation type they produce.
pub mod source;

/// Sanity checks applied between fetching and committing.
pub mod guard;

/// Diffing a fresh crawl against the live rate card.
pub mod diff;

/// Crawler instruments recorded to the OpenTelemetry global meter.
pub mod metrics;

/// Reading `icegate.prices` back into the live rate card.
pub mod read;

/// Encoding observations to Arrow and committing them to `icegate.prices`.
pub mod writer;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use iceberg::Catalog;
use jobmanager::{
    CachedStorage, Error as JobError, ImmutableTask, JobCode, JobDefinition, JobDefinitionRegistry, JobManager,
    JobRegistry, JobsManager, JobsManagerConfig, JobsManagerHandle, Metrics as JobMetrics, S3Storage, TaskCode,
    TaskDefinition, TaskExecutorFn, WorkerConfig,
};
use reqwest::Client;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::error::{MaintainError, Result};

/// Task code for the single pricing crawl task.
pub const PRICING_TASK_CODE: &str = "pricing";
/// Job name registered with the jobmanager.
const PRICING_JOB_NAME: &str = "pricing";
/// Number of highest-priced rows the bootstrap WARN summary includes.
const BOOTSTRAP_SUMMARY_ROW_COUNT: usize = 10;

/// Construct the configured sources.
///
/// Every source honours its entry's `owns` override, and the result is checked
/// for overlapping ownership: two sources claiming one provider would both emit
/// rows for the same key, and the table would flap between them on alternate
/// crawls. Disjointness is the invariant that lets the rest of the crawler skip
/// precedence logic entirely, so it is enforced here rather than assumed.
///
/// # Errors
///
/// Returns [`MaintainError::Config`] if a config entry names a source with no
/// implementation, or if two configured sources claim the same provider.
fn build_sources(config: &config::PricingConfig) -> Result<Vec<Box<dyn source::PriceSource>>> {
    let sources = config
        .sources
        .iter()
        .map(|entry| -> Result<Box<dyn source::PriceSource>> {
            let owns = entry.owns.clone().unwrap_or_default();
            match entry.name.as_str() {
                "openrouter" => Ok(Box::new(source::openrouter::OpenRouterSource::new(
                    entry.url.clone(),
                    owns,
                    config.max_response_bytes,
                ))),
                "litellm" => Ok(Box::new(source::litellm::LiteLlmSource::new(
                    entry.url.clone(),
                    owns,
                    config.max_response_bytes,
                ))),
                "bedrock" => Ok(Box::new(source::bedrock::BedrockSource::new(
                    entry.url.clone(),
                    owns,
                    config.max_response_bytes,
                ))),
                other => Err(MaintainError::Config(format!(
                    "pricing.sources: no implementation for source '{other}'"
                ))),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut claimed: HashMap<String, &'static str> = HashMap::new();
    for source_impl in &sources {
        for provider in source_impl.owned_providers() {
            if let Some(other) = claimed.insert(provider.clone(), source_impl.name()) {
                return Err(MaintainError::Config(format!(
                    "pricing.sources: provider '{provider}' is claimed by both '{other}' and '{}'; \
                     use `owns` to give it to exactly one source",
                    source_impl.name()
                )));
            }
        }
    }
    Ok(sources)
}

/// Select the `n` observations with the highest `output_usd_per_1m`, dearest
/// first. An observation with no output rate sorts as if it were `0.0`, so a
/// rate-less row never crowds out a priced one.
///
/// Pure and catalog-free so the bootstrap summary's "highest, not
/// first-N-encountered" selection is unit-testable without running a crawl.
fn select_highest_output_rates(accepted: &[source::RateObservation], n: usize) -> Vec<&source::RateObservation> {
    let mut dearest: Vec<&source::RateObservation> = accepted.iter().collect();
    dearest.sort_by(|a, b| {
        b.output_usd_per_1m
            .unwrap_or(0.0)
            .total_cmp(&a.output_usd_per_1m.unwrap_or(0.0))
    });
    dearest.truncate(n);
    dearest
}

/// Log the bootstrap summary for a source that has no live rows to compare
/// against — the first crawl of a cluster, or the first crawl after a source is
/// added to a populated table.
///
/// Guards 5 and 6 both compare against live state, so such a batch is
/// unguarded against a plausible-but-wrong rate — row guards 3/4/7 still
/// reject absurd, malformed, and non-USD values. This summary is what an
/// operator spot-checks against a vendor pricing page, once, when bringing the
/// source up.
fn log_bootstrap_summary(source_name: &str, accepted: &[source::RateObservation]) {
    let sample: Vec<String> = select_highest_output_rates(accepted, BOOTSTRAP_SUMMARY_ROW_COUNT)
        .into_iter()
        .map(|r| format!("{}/{}={:?}", r.provider, r.model, r.output_usd_per_1m))
        .collect();
    tracing::warn!(
        source = source_name,
        row_count = accepted.len(),
        highest_output_rates = ?sample,
        "bootstrap pricing crawl: no live rates to compare against, so the \
         cardinality and delta guards did not run. Spot-check these against the \
         vendor's pricing page once."
    );
}

/// What one source contributed to a crawl.
///
/// A source that contributes nothing leaves its live rates in effect, which is
/// the right per-source behaviour but indistinguishable from "prices did not
/// move" once the contributions are pooled. Carrying the reason lets
/// [`run_crawl`] tell the two apart when *every* source contributes nothing.
enum SourceContribution {
    /// Rows that survived ownership scoping and every guard; never empty.
    Rates(Vec<source::RateObservation>),
    /// The source supplied no usable rows, for this reason.
    Nothing(String),
}

/// Source-independent context [`process_source`] needs, bundled so its
/// parameter count stays within the workspace's `too-many-arguments`
/// threshold.
struct CrawlContext<'a> {
    /// The rate card in effect before this crawl.
    live: &'a diff::LiveRates,
    config: &'a config::PricingConfig,
    metrics: &'a metrics::PricingMetrics,
    now: DateTime<Utc>,
}

/// Guard and collect one source's fetch result into candidates to pool.
///
/// Implements crawl-order steps 3a-3e (see the module doc): a source that
/// errored contributes nothing and does not fail the crawl (3a); rows for a
/// provider the source does not own are dropped (3b); a source that fails the
/// cardinality guard has its entire batch discarded, not individual rows
/// (3c); the remaining rows pass the row guards (3d); and on the bootstrap
/// crawl, a non-empty accepted batch is summarised at WARN (3e).
fn process_source(
    source_impl: &dyn source::PriceSource,
    elapsed_secs: f64,
    result: Result<Vec<source::RateObservation>>,
    ctx: &CrawlContext<'_>,
) -> SourceContribution {
    let name = source_impl.name();
    ctx.metrics.record_duration(name, elapsed_secs);

    let rates = match result {
        Ok(rates) => rates,
        Err(error) => {
            tracing::error!(source = name, %error, "pricing source failed; its live rates stay in effect");
            return SourceContribution::Nothing(format!("fetch failed: {error}"));
        }
    };

    // Ownership is structural: a source's rows for a provider it does not own
    // are discarded, so two sources can never emit the same key.
    let owned: HashSet<String> = source_impl.owned_providers().into_iter().collect();
    let scoped: Vec<source::RateObservation> = rates.into_iter().filter(|r| owned.contains(&r.provider)).collect();

    // Bootstrap is per source, not per crawl: `check_cardinality` no-ops on a
    // zero live count, so a source newly added to an already-populated table is
    // just as unguarded as the very first crawl and needs the same WARN.
    let live_count = ctx.live.count_for_source(name);
    if let Err(reason) = guard::check_cardinality(name, scoped.len(), live_count, ctx.config) {
        ctx.metrics.record_rejected(name, reason.as_str(), scoped.len() as u64);
        return SourceContribution::Nothing(format!("cardinality guard discarded the batch: {}", reason.as_str()));
    }

    let scoped_count = scoped.len();
    let outcome = guard::apply_row_guards(scoped);
    for (_, reason) in &outcome.rejected {
        ctx.metrics.record_rejected(name, reason.as_str(), 1);
    }
    ctx.metrics.record_models_live(name, outcome.accepted.len() as u64);

    if live_count == 0 && !outcome.accepted.is_empty() {
        log_bootstrap_summary(name, &outcome.accepted);
    }

    // Recorded before the emptiness check: the fetch itself succeeded, which is
    // what this instrument tracks. Whether the rows survived the guards is
    // `record_rejected`'s business.
    ctx.metrics.record_success(name, ctx.now.timestamp());

    if outcome.accepted.is_empty() {
        return SourceContribution::Nothing(if scoped_count == 0 {
            "no rows for the providers it owns".to_string()
        } else {
            format!("all {scoped_count} owned rows were rejected by the row guards")
        });
    }
    SourceContribution::Rates(outcome.accepted)
}

/// Drop every candidate whose [`source::RateKey`] a preceding candidate already
/// claimed, keeping the first.
///
/// Cross-source collisions are already impossible (ownership is disjoint, and
/// [`build_sources`] enforces it), but a single source can still emit one key
/// twice — for example when two upstream model keys reduce to the same
/// `(provider, model)` after prefix-stripping. Both rows would be appended in
/// one commit with the same `valid_from`, after which [`diff::LiveRates`] keeps
/// one arbitrarily and the other is re-appended on every subsequent crawl:
/// unbounded row growth and a price that alternates. Dropping the duplicate
/// here keeps that failure out of the table whatever a source does.
fn retain_first_per_key(candidates: &mut Vec<source::RateObservation>) {
    let mut seen: HashSet<source::RateKey> = HashSet::with_capacity(candidates.len());
    candidates.retain(|candidate| {
        if seen.insert(candidate.key()) {
            return true;
        }
        tracing::warn!(
            source = %candidate.source,
            provider = %candidate.provider,
            model = %candidate.model,
            region = %candidate.region,
            min_input_tokens = candidate.min_input_tokens,
            "pricing source emitted the same rate key twice; keeping the first and dropping this row"
        );
        false
    });
}

/// Fetch, guard, diff, and commit one crawl.
///
/// See the module doc for the full eight-step order this function implements.
///
/// # Errors
///
/// Returns [`MaintainError::Upstream`] if no source contributed a single row,
/// and otherwise whatever reading the live card or committing the append
/// returns. An individual source failing is not an error: its live rates stay
/// in effect and the crawl proceeds on the rest.
async fn run_crawl(
    catalog: &Arc<dyn Catalog>,
    client: &Client,
    sources: &[Box<dyn source::PriceSource>],
    config: &config::PricingConfig,
    metrics: &metrics::PricingMetrics,
    now: DateTime<Utc>,
) -> Result<()> {
    let live = read::read_live_rates(catalog)
        .instrument(info_span!("pricing_read_live_rates"))
        .await?;
    let ctx = CrawlContext {
        live: &live,
        config,
        metrics,
        now,
    };

    // One span per source: the fetches run concurrently, so a slow or hanging
    // feed is only distinguishable from a slow crawl when each has its own span.
    let fetched = futures::future::join_all(sources.iter().map(|s| {
        let span = info_span!("pricing_fetch_source", source = s.name());
        async move {
            let started = std::time::Instant::now();
            let result = s.fetch_rates(client, now).await;
            (s.as_ref(), started.elapsed().as_secs_f64(), result)
        }
        .instrument(span)
    }))
    .await;

    let mut candidates: Vec<source::RateObservation> = Vec::new();
    let mut barren: Vec<String> = Vec::new();
    for (source_impl, elapsed_secs, result) in fetched {
        match process_source(source_impl, elapsed_secs, result, &ctx) {
            SourceContribution::Rates(rates) => candidates.extend(rates),
            SourceContribution::Nothing(reason) => barren.push(format!("{}: {reason}", source_impl.name())),
        }
    }

    // An empty pool is not a no-change crawl: nothing was observed at all, so
    // the stale live card would be reported as current and the task would go
    // green. Fail instead, naming every source and why it gave nothing — an
    // all-sources-down crawl and a genuinely unchanged rate card are otherwise
    // indistinguishable from the job status.
    if candidates.is_empty() {
        return Err(MaintainError::Upstream(format!(
            "pricing crawl observed no rates from any of {} sources ({})",
            barren.len(),
            barren.join("; ")
        )));
    }

    retain_first_per_key(&mut candidates);

    canonical::apply_canonical_ids(&mut candidates);
    let missing = candidates.iter().filter(|r| r.canonical_id.is_none()).count();
    metrics.record_canonical_missing(missing as u64);

    // Snap fresh rates onto the stored decimal grid before diffing. Live rates
    // read back from `Decimal(38,10)` storage are already on the grid, so an
    // un-quantized fresh `3.0000000000000004` would look like a change and append
    // a spurious revision every crawl. See `decimal::quantize_observation`.
    for candidate in &mut candidates {
        decimal::quantize_observation(candidate);
    }

    let diff_outcome = diff::diff_rates(candidates, &live, config, now);
    for (rate, reason) in &diff_outcome.rejected {
        metrics.record_rejected(&rate.source, reason.as_str(), 1);
    }

    // Steady state: most crawls change nothing, so nothing is committed and no
    // empty snapshot is created (see `writer::append_prices`).
    if diff_outcome.to_append.is_empty() {
        tracing::debug!("pricing crawl complete: no rate changes");
        return Ok(());
    }

    writer::append_prices(catalog, &diff_outcome.to_append)
        .instrument(info_span!("pricing_append", rows = diff_outcome.to_append.len()))
        .await?;
    for rate in &diff_outcome.to_append {
        metrics.record_appended(&rate.source, 1);
    }
    tracing::info!(
        appended = diff_outcome.to_append.len(),
        "pricing crawl committed rate changes"
    );
    Ok(())
}

/// The pricing crawl task executor.
struct PricingExecutor {
    catalog: Arc<dyn Catalog>,
    client: Client,
    sources: Vec<Box<dyn source::PriceSource>>,
    config: config::PricingConfig,
    metrics: metrics::PricingMetrics,
}

impl PricingExecutor {
    async fn run(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        _cancel: &CancellationToken,
    ) -> std::result::Result<(), JobError> {
        let span = info_span!("pricing_crawl");
        self.run_crawl_task(task, manager).instrument(span).await
    }

    async fn run_crawl_task(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
    ) -> std::result::Result<(), JobError> {
        let now = Utc::now();
        run_crawl(
            &self.catalog,
            &self.client,
            &self.sources,
            &self.config,
            &self.metrics,
            now,
        )
        .await
        .map_err(|e| JobError::TaskExecution(format!("pricing crawl failed: {e}")))?;
        manager.complete_task(task.id(), Vec::new())
    }
}

/// Wrap a [`PricingExecutor`] as a jobmanager [`TaskExecutorFn`].
fn pricing_executor_fn(executor: Arc<PricingExecutor>) -> TaskExecutorFn {
    Arc::new(move |task, manager, cancel| {
        let executor = Arc::clone(&executor);
        Box::pin(async move { executor.run(task.as_ref(), manager, &cancel).await })
    })
}

/// Runs the LLM pricing crawler inside the maintain process.
pub struct PricingRunner {
    manager: JobsManager,
}

/// Handle for draining a running [`PricingRunner`].
pub struct PricingRunnerHandle {
    handle: JobsManagerHandle,
}

impl PricingRunner {
    /// Build a runner with the single pricing crawl job.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if the config is invalid (this is the only
    /// place [`config::PricingConfig`] is validated — see its doc comment and
    /// [`crate::config::MaintainConfig::validate`]), a configured source name
    /// has no implementation, or the jobmanager storage cannot be constructed.
    pub async fn new(catalog: Arc<dyn Catalog>, config: &config::PricingConfig) -> Result<Self> {
        Self::new_with_max_iterations(catalog, config, None).await
    }

    /// Test seam: like [`Self::new`] but caps the job at `max_iterations`
    /// discovery cycles.
    ///
    /// # Errors
    ///
    /// See [`Self::new`].
    #[doc(hidden)]
    pub async fn new_with_max_iterations(
        catalog: Arc<dyn Catalog>,
        config: &config::PricingConfig,
        max_iterations: Option<u64>,
    ) -> Result<Self> {
        config.validate()?;
        let sources = build_sources(config)?;

        // The crawl cadence is `pricing.interval_secs`, NOT the jobmanager's own
        // `scan_interval_secs`: this job has a single task, so its discovery loop
        // period *is* the crawl period, and reading the jobmanager field would
        // silently ignore the interval the operator configured.
        let interval_secs = i64::try_from(config.interval_secs)
            .map_err(|_| MaintainError::Config("pricing.interval_secs is too large".to_string()))?;
        let interval = ChronoDuration::seconds(interval_secs);
        // The task deadline is its own knob, not the per-source HTTP timeout:
        // sources fetch concurrently (see `run_crawl`), so a crawl costs one HTTP
        // round trip plus the guard/diff/append tail, and a task that overruns its
        // deadline becomes pickup-eligible again — a second worker would crawl
        // concurrently and append every revision twice.
        let crawl_timeout_secs = i64::try_from(config.crawl_timeout_secs)
            .map_err(|_| MaintainError::Config("pricing.crawl_timeout_secs is too large".to_string()))?;
        let timeout = ChronoDuration::seconds(crawl_timeout_secs);

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| MaintainError::Config(format!("pricing: failed to build HTTP client: {e}")))?;

        let metrics = metrics::PricingMetrics::new();
        let executor = Arc::new(PricingExecutor {
            catalog: Arc::clone(&catalog),
            client,
            sources,
            config: config.clone(),
            metrics,
        });

        let initial =
            TaskDefinition::new(TaskCode::new(PRICING_TASK_CODE), Vec::new(), timeout).map_err(map_job_error)?;
        let mut executors: HashMap<TaskCode, TaskExecutorFn> = HashMap::new();
        executors.insert(TaskCode::new(PRICING_TASK_CODE), pricing_executor_fn(executor));

        let mut job_def = JobDefinition::new(JobCode::new(PRICING_JOB_NAME), vec![initial], executors)
            .map_err(map_job_error)?
            .with_iteration_interval(interval)
            .map_err(map_job_error)?;
        if let Some(max) = max_iterations {
            job_def = job_def.with_max_iterations(max).map_err(map_job_error)?;
        }

        let job_registry = Arc::new(JobRegistry::new(vec![job_def]).map_err(map_job_error)?);
        // Enable the jobmanager's own metrics (job/task durations and statuses,
        // task-steal events, optimistic-concurrency save retries, and job-state
        // storage S3 latency / cache hits), mirroring GcRunner/Compactor: binds to
        // the global meter installed by `MetricsRuntime`; inert when metrics are
        // disabled (no provider set).
        let job_metrics = JobMetrics::new(&opentelemetry::global::meter("icegate-maintain"));
        let registry_dyn: Arc<dyn JobDefinitionRegistry> = job_registry.clone();
        let s3_storage = Arc::new(
            S3Storage::new(
                config.jobsmanager.storage.to_s3_storage_config()?,
                registry_dyn,
                job_metrics.clone(),
            )
            .await
            .map_err(map_job_error)?,
        );
        let cached_storage = Arc::new(CachedStorage::new(s3_storage, job_metrics.clone()));

        let manager_config = JobsManagerConfig {
            worker_count: config.jobsmanager.worker_count,
            worker_config: WorkerConfig {
                poll_interval: std::time::Duration::from_millis(config.jobsmanager.poll_interval_ms),
                ..Default::default()
            },
        };
        let manager =
            JobsManager::new(cached_storage, manager_config, job_registry, job_metrics).map_err(map_job_error)?;
        Ok(Self { manager })
    }

    /// Start the pricing crawler. Returns a handle that drains it on shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if the jobmanager fails to start.
    pub fn start(&self) -> Result<PricingRunnerHandle> {
        let handle = self.manager.start().map_err(map_job_error)?;
        Ok(PricingRunnerHandle { handle })
    }
}

impl PricingRunnerHandle {
    /// Cancel the workers and wait for them to drain.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if a worker stopped with an error.
    pub async fn shutdown(self) -> Result<()> {
        self.handle.shutdown().await.map_err(map_job_error)
    }
}

/// Map any [`Display`](std::fmt::Display) error (jobmanager constructors,
/// duration overflow) into [`MaintainError::Config`].
fn map_job_error<E: std::fmt::Display>(error: E) -> MaintainError {
    MaintainError::Config(error.to_string())
}

#[cfg(test)]
mod tests {
    // `field_reassign_with_default` is allowed here for the same reason as in
    // `pricing::config::tests`: these tests build a `PricingConfig::default()`
    // and then override only the field(s) under test.
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::field_reassign_with_default)]

    use std::collections::HashMap;

    use chrono::{DateTime, Utc};

    use super::MaintainError;
    use super::config::{PricingConfig, SourceConfig};
    use super::{PricingRunner, build_sources, retain_first_per_key, select_highest_output_rates};
    use crate::pricing::source::{RateObservation, ValidFromSource};

    #[test]
    fn build_sources_constructs_one_source_per_config_entry() {
        let config = PricingConfig::default();
        let sources = build_sources(&config).expect("sources build");
        assert_eq!(sources.len(), 2);
        let names: Vec<&str> = sources.iter().map(|s| s.name()).collect();
        assert!(names.contains(&"openrouter"));
        assert!(names.contains(&"litellm"));
    }

    #[test]
    fn build_sources_rejects_an_unknown_source_name() {
        // Fail at construction, not silently at crawl time.
        let mut config = PricingConfig::default();
        config.sources[0].name = "not-a-real-source".to_string();
        assert!(build_sources(&config).is_err());
    }

    #[test]
    fn owned_providers_do_not_overlap_across_sources() {
        // The invariant that makes precedence structural: if two sources claimed
        // one provider they would flap between crawls.
        let config = PricingConfig::default();
        let sources = build_sources(&config).expect("sources build");
        let mut seen: Vec<String> = Vec::new();
        for source in &sources {
            for provider in source.owned_providers() {
                assert!(!seen.contains(&provider), "provider '{provider}' claimed twice");
                seen.push(provider);
            }
        }
    }

    #[test]
    fn build_sources_rejects_two_sources_claiming_one_provider() {
        // `owns` can hand a provider to another source; if the source that owns
        // it by default is still configured, both emit rows for the same key and
        // the table flaps between them on alternate crawls. The disjointness
        // that lets the rest of the crawler skip precedence logic entirely has
        // to be checked, not assumed.
        let mut config = PricingConfig::default();
        config.sources.push(SourceConfig {
            name: "bedrock".to_string(),
            url: "https://pricing.example/AmazonBedrock/current/index.json".to_string(),
            owns: None,
        });
        // Hand `aws.bedrock` to litellm while the bedrock source still claims it.
        config.sources[1].owns = Some(vec!["aws.bedrock".to_string()]);
        assert!(build_sources(&config).is_err());
    }

    #[test]
    fn build_sources_honours_owns_on_every_source_not_just_litellm() {
        let mut config = PricingConfig::default();
        config.sources[0].owns = Some(vec!["some.reseller".to_string()]);
        let sources = build_sources(&config).expect("sources build");
        let openrouter = sources.iter().find(|s| s.name() == "openrouter").expect("openrouter");
        assert_eq!(openrouter.owned_providers(), vec!["some.reseller".to_string()]);
    }

    #[test]
    fn retain_first_per_key_drops_a_repeated_key() {
        // Two rows with one key in a single batch are both appended with the
        // same `valid_from`; `LiveRates` then keeps one arbitrarily and the
        // other is re-appended on every subsequent crawl.
        let mut candidates = vec![
            observation_with_output_rate("dup", Some(1.0)),
            observation_with_output_rate("dup", Some(2.0)),
            observation_with_output_rate("other", Some(3.0)),
        ];
        retain_first_per_key(&mut candidates);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].output_usd_per_1m, Some(1.0), "the first occurrence wins");
        assert_eq!(candidates[1].model, "other");
    }

    /// Catalog stub that panics if touched. `PricingRunner::new` must reject an
    /// invalid config via `config.validate()` before ever using its catalog
    /// argument, so this proves that ordering without needing a real catalog.
    #[derive(Debug)]
    struct UnreachableCatalog;

    #[async_trait::async_trait]
    impl iceberg::Catalog for UnreachableCatalog {
        async fn list_namespaces(
            &self,
            _parent: Option<&iceberg::NamespaceIdent>,
        ) -> iceberg::Result<Vec<iceberg::NamespaceIdent>> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn create_namespace(
            &self,
            _namespace: &iceberg::NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn get_namespace(&self, _namespace: &iceberg::NamespaceIdent) -> iceberg::Result<iceberg::Namespace> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn namespace_exists(&self, _namespace: &iceberg::NamespaceIdent) -> iceberg::Result<bool> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn update_namespace(
            &self,
            _namespace: &iceberg::NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn drop_namespace(&self, _namespace: &iceberg::NamespaceIdent) -> iceberg::Result<()> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn list_tables(&self, _namespace: &iceberg::NamespaceIdent) -> iceberg::Result<Vec<iceberg::TableIdent>> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn create_table(
            &self,
            _namespace: &iceberg::NamespaceIdent,
            _creation: iceberg::TableCreation,
        ) -> iceberg::Result<iceberg::table::Table> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn load_table(&self, _table: &iceberg::TableIdent) -> iceberg::Result<iceberg::table::Table> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn drop_table(&self, _table: &iceberg::TableIdent) -> iceberg::Result<()> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn table_exists(&self, _table: &iceberg::TableIdent) -> iceberg::Result<bool> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn rename_table(&self, _src: &iceberg::TableIdent, _dest: &iceberg::TableIdent) -> iceberg::Result<()> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn register_table(
            &self,
            _table: &iceberg::TableIdent,
            _metadata_location: String,
        ) -> iceberg::Result<iceberg::table::Table> {
            unreachable!("catalog must not be touched before config validation")
        }

        async fn update_table(&self, _commit: iceberg::TableCommit) -> iceberg::Result<iceberg::table::Table> {
            unreachable!("catalog must not be touched before config validation")
        }
    }

    #[tokio::test]
    async fn pricing_runner_new_rejects_an_invalid_config() {
        // Proves `PricingRunner::new` is the enforcement point for
        // `PricingConfig::validate` — `MaintainConfig::validate` deliberately
        // skips it (see that method's doc comment).
        let mut config = PricingConfig::default();
        config.enabled = true;
        config.interval_secs = 0;

        let catalog: std::sync::Arc<dyn iceberg::Catalog> = std::sync::Arc::new(UnreachableCatalog);
        let result = PricingRunner::new(catalog, &config).await;
        assert!(result.is_err());
    }

    fn observation_with_output_rate(model: &str, output: Option<f64>) -> RateObservation {
        RateObservation {
            provider: "anthropic".to_string(),
            model: model.to_string(),
            canonical_id: None,
            service_tier: "standard".to_string(),
            region: "global".to_string(),
            min_input_tokens: 0,
            max_input_tokens: None,
            valid_from: DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp"),
            valid_from_source: ValidFromSource::Observed,
            input_usd_per_1m: Some(1.0),
            output_usd_per_1m: output,
            cache_read_usd_per_1m: None,
            cache_write_usd_per_1m: None,
            reasoning_usd_per_1m: None,
            request_usd: None,
            image_input_usd_per_unit: None,
            image_output_usd_per_unit: None,
            audio_input_usd_per_second: None,
            audio_output_usd_per_second: None,
            currency: "USD".to_string(),
            source: "litellm".to_string(),
            source_url: None,
        }
    }

    #[test]
    fn select_highest_output_rates_picks_the_dearest_not_the_first_encountered() {
        // Listed cheapest-first: a bug that took the first N encountered rather
        // than sorting would return this in input order instead of by price.
        let rates = vec![
            observation_with_output_rate("cheap", Some(1.0)),
            observation_with_output_rate("dearest", Some(50.0)),
            observation_with_output_rate("middle", Some(25.0)),
        ];
        let top = select_highest_output_rates(&rates, 2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].model, "dearest");
        assert_eq!(top[1].model, "middle");
    }

    #[test]
    fn select_highest_output_rates_treats_a_missing_rate_as_zero() {
        // A rate-less row must never crowd out a priced one at the top of the
        // summary; it also must not panic the total_cmp on a None -> unwrap_or.
        let rates = vec![
            observation_with_output_rate("rateless", None),
            observation_with_output_rate("priced", Some(5.0)),
        ];
        let top = select_highest_output_rates(&rates, 1);
        assert_eq!(top[0].model, "priced");
    }

    #[test]
    fn select_highest_output_rates_truncates_to_n() {
        let rates = vec![
            observation_with_output_rate("a", Some(1.0)),
            observation_with_output_rate("b", Some(2.0)),
            observation_with_output_rate("c", Some(3.0)),
        ];
        assert_eq!(select_highest_output_rates(&rates, 2).len(), 2);
        assert_eq!(select_highest_output_rates(&rates, 10).len(), 3);
    }

    /// A source implementation whose only job is to have a name and an owned
    /// provider; `process_source` is handed the fetch result directly, so this
    /// never fetches.
    struct NamedSource;

    #[async_trait::async_trait]
    impl super::source::PriceSource for NamedSource {
        fn name(&self) -> &'static str {
            "litellm"
        }
        fn owned_providers(&self) -> Vec<String> {
            vec!["anthropic".to_string()]
        }
        async fn fetch_rates(
            &self,
            _client: &reqwest::Client,
            _now: DateTime<Utc>,
        ) -> crate::error::Result<Vec<RateObservation>> {
            unreachable!("process_source is given the fetch result, it does not fetch")
        }
    }

    /// The pieces a [`super::CrawlContext`] borrows, over an empty live card —
    /// the bootstrap case, where the cardinality guard no-ops and only the row
    /// guards can reject. Returned rather than assembled here so the caller
    /// owns them for as long as the context borrows them.
    fn bootstrap_context_parts() -> (super::diff::LiveRates, PricingConfig, super::metrics::PricingMetrics) {
        (
            super::diff::LiveRates::from_rows(Vec::new()),
            PricingConfig::default(),
            super::metrics::PricingMetrics::new(),
        )
    }

    fn crawl_context<'a>(
        live: &'a super::diff::LiveRates,
        config: &'a PricingConfig,
        metrics: &'a super::metrics::PricingMetrics,
    ) -> super::CrawlContext<'a> {
        super::CrawlContext {
            live,
            config,
            metrics,
            now: DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp"),
        }
    }

    #[test]
    fn a_source_that_supplies_rows_contributes_them() {
        let (live, config, metrics) = bootstrap_context_parts();
        let ctx = crawl_context(&live, &config, &metrics);

        let contribution = super::process_source(
            &NamedSource,
            0.1,
            Ok(vec![observation_with_output_rate("claude-opus-4-8", Some(25.0))]),
            &ctx,
        );

        match contribution {
            super::SourceContribution::Rates(rates) => assert_eq!(rates.len(), 1),
            super::SourceContribution::Nothing(reason) => panic!("expected rates, got nothing: {reason}"),
        }
    }

    #[test]
    fn a_source_that_supplies_nothing_carries_the_reason_why() {
        // The reason is what makes an all-sources-dark crawl distinguishable
        // from an unchanged rate card: `run_crawl` fails with these strings
        // rather than reporting a quiet, successful no-change crawl.
        let (live, config, metrics) = bootstrap_context_parts();
        let ctx = crawl_context(&live, &config, &metrics);

        let failed = super::process_source(
            &NamedSource,
            0.1,
            Err(MaintainError::Storage("connection refused".to_string())),
            &ctx,
        );
        assert!(matches!(failed, super::SourceContribution::Nothing(_)));

        // A 200 response carrying only providers this source does not own is
        // just as barren as a failed fetch, and must not read as success.
        let unowned = observation_with_output_rate("gpt-5", Some(25.0));
        let disowned = super::process_source(
            &NamedSource,
            0.1,
            Ok(vec![RateObservation {
                provider: "openai".to_string(),
                ..unowned
            }]),
            &ctx,
        );
        assert!(matches!(disowned, super::SourceContribution::Nothing(_)));
    }
}
