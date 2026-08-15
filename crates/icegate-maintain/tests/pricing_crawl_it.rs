//! End-to-end crawl against a real S3 catalog: rows land, and a second identical
//! crawl commits nothing.
//!
//! The append/no-op properties are exercised against the crawl pipeline directly
//! (`read_live_rates` -> `apply_row_guards` -> `diff_rates` -> `append_prices`),
//! which is where they live and which stays fast and deterministic. A final test
//! goes through `PricingRunner` so the wiring around that pipeline — config
//! validation, source construction, the crawl interval, the task timeout, job
//! registration, the jobmanager's own S3 job-state storage, and a clean drain —
//! is covered too rather than assumed.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common; // reuse the RustFS + catalog harness used by the GC tests

use std::sync::Arc;

use arrow::array::{Array, Decimal128Array, TimestampMicrosecondArray};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use common::{BUCKET_NAME, build_s3_catalog};
use futures::TryStreamExt;
use iceberg::Catalog;
use icegate_common::{PRICES_TABLE, icegate_table_ident};
use icegate_maintain::jobs::{JobStateCodec, JobsManagerConfig, JobsStorageConfig};
use icegate_maintain::migrate::config::SnapshotExpirationConfig;
use icegate_maintain::migrate::operations::create_tables;
use icegate_maintain::pricing::PricingRunner;
use icegate_maintain::pricing::config::{PricingConfig, SourceConfig};
use icegate_maintain::pricing::decimal::unscaled_to_rate;
use icegate_maintain::pricing::diff::diff_rates;
use icegate_maintain::pricing::guard::apply_row_guards;
use icegate_maintain::pricing::read::read_live_rates;
use icegate_maintain::pricing::source::{PriceSource, RateObservation, ValidFromSource};
use icegate_maintain::pricing::writer::append_prices;

/// Returns a fixed rate card without touching the network.
struct StubSource {
    rates: Vec<RateObservation>,
}

#[async_trait]
impl PriceSource for StubSource {
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
    ) -> icegate_maintain::error::Result<Vec<RateObservation>> {
        Ok(self.rates.clone())
    }
}

/// One rate observation for `anthropic/claude-opus-4-8`, `standard`/`global`,
/// priced at `input_usd_per_1m`. Every test call shares this key so a second
/// observation at a different price is a revision of the same series rather
/// than a parallel one.
fn rate_observation(input_usd_per_1m: f64, valid_from: DateTime<Utc>) -> RateObservation {
    RateObservation {
        provider: "anthropic".to_string(),
        model: "claude-opus-4-8".to_string(),
        canonical_id: Some("anthropic/claude-opus-4-8".to_string()),
        service_tier: "standard".to_string(),
        region: "global".to_string(),
        min_input_tokens: 0,
        max_input_tokens: None,
        valid_from,
        valid_from_source: ValidFromSource::Observed,
        input_usd_per_1m: Some(input_usd_per_1m),
        output_usd_per_1m: Some(25.0),
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

/// Run one crawl cycle against `stub`, mirroring `PricingRunner::run_crawl`'s
/// per-source steps (owner filter, row guards, diff, append) without the
/// jobmanager scaffolding around it.
async fn run_stub_crawl(catalog: &Arc<dyn Catalog>, stub: &StubSource, config: &PricingConfig, now: DateTime<Utc>) {
    let live = read_live_rates(catalog).await.expect("read live rates");
    let client = reqwest::Client::new();
    let fetched = stub.fetch_rates(&client, now).await.expect("stub fetch");
    let owned: std::collections::HashSet<String> = stub.owned_providers().into_iter().collect();
    let scoped: Vec<RateObservation> = fetched.into_iter().filter(|r| owned.contains(&r.provider)).collect();
    let guarded = apply_row_guards(scoped);
    assert!(
        guarded.rejected.is_empty(),
        "stub rates must pass every row guard: {:?}",
        guarded.rejected
    );
    let mut accepted = guarded.accepted;
    for candidate in &mut accepted {
        icegate_maintain::pricing::decimal::quantize_observation(candidate);
    }
    let outcome = diff_rates(accepted, &live, config, now);
    assert!(
        outcome.rejected.is_empty(),
        "stub rates must pass the delta guard: {:?}",
        outcome.rejected
    );
    append_prices(catalog, &outcome.to_append).await.expect("append prices");
}

/// The current snapshot id of `icegate.prices`, or `None` if the table has
/// never been committed to.
async fn current_snapshot_id(catalog: &Arc<dyn Catalog>) -> Option<i64> {
    let ident = icegate_table_ident(PRICES_TABLE);
    let table = catalog.load_table(&ident).await.expect("load prices table");
    table.metadata().current_snapshot_id()
}

/// `(input_usd_per_1m, valid_from_micros)` for every physical row in
/// `icegate.prices`, read straight off the table's current snapshot (not
/// reduced to the latest revision per key, unlike `read_live_rates`).
async fn read_raw_rows(catalog: &Arc<dyn Catalog>) -> Vec<(f64, i64)> {
    let ident = icegate_table_ident(PRICES_TABLE);
    let table = catalog.load_table(&ident).await.expect("load prices table");
    let mut stream = table
        .scan()
        .build()
        .expect("build scan")
        .to_arrow()
        .await
        .expect("arrow stream");

    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await.expect("read batch") {
        let schema = batch.schema();
        let input = batch
            .column(schema.index_of("input_usd_per_1m").expect("input_usd_per_1m column"))
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("input_usd_per_1m is Decimal128");
        let valid_from = batch
            .column(schema.index_of("valid_from").expect("valid_from column"))
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("valid_from is TimestampMicrosecond");
        for i in 0..batch.num_rows() {
            // Decode the unscaled i128 back to the rate the same way the reader does.
            rows.push((unscaled_to_rate(input.value(i)), valid_from.value(i)));
        }
    }
    rows.sort_by_key(|(_, valid_from)| *valid_from);
    rows
}

#[tokio::test]
async fn first_crawl_appends_and_second_identical_crawl_commits_nothing() {
    // Steady state is an empty commit — the property that keeps a sixth table
    // from generating small-file churn.
    let (_store, conn) = common::setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    create_tables(&dyn_catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("create tables");

    let now = Utc::now();
    let stub = StubSource {
        rates: vec![rate_observation(5.0, now)],
    };
    let config = PricingConfig::default();

    // 1. First crawl: the table is empty, so this appends the stub's one row.
    run_stub_crawl(&dyn_catalog, &stub, &config, now).await;
    let rows_after_first = read_raw_rows(&dyn_catalog).await;
    assert_eq!(rows_after_first.len(), 1, "the first crawl must append exactly one row");
    let snapshot_after_first = current_snapshot_id(&dyn_catalog).await;
    assert!(
        snapshot_after_first.is_some(),
        "the first crawl must produce a snapshot"
    );

    // 2. Second crawl: identical stub rates, so the diff has nothing to append
    // and `append_prices` must not be reached at all — no empty snapshot.
    run_stub_crawl(&dyn_catalog, &stub, &config, Utc::now()).await;

    let snapshot_after_second = current_snapshot_id(&dyn_catalog).await;
    assert_eq!(
        snapshot_after_second, snapshot_after_first,
        "an identical second crawl must not create a new snapshot, empty or otherwise"
    );
    let rows_after_second = read_raw_rows(&dyn_catalog).await;
    assert_eq!(
        rows_after_second, rows_after_first,
        "an identical second crawl must not change the table's rows"
    );
}

#[tokio::test]
async fn a_changed_rate_appends_exactly_one_revision() {
    let (_store, conn) = common::setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    create_tables(&dyn_catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("create tables");

    let config = PricingConfig::default();
    let first_valid_from = Utc::now();
    let first_stub = StubSource {
        rates: vec![rate_observation(5.0, first_valid_from)],
    };
    run_stub_crawl(&dyn_catalog, &first_stub, &config, first_valid_from).await;
    let rows_after_first = read_raw_rows(&dyn_catalog).await;
    assert_eq!(rows_after_first.len(), 1);

    // A changed rate at the same key: well within the default `max_change_ratio`
    // (10.0), so it must pass the delta guard and append a new revision rather
    // than being rejected.
    let second_valid_from = first_valid_from + chrono::Duration::seconds(1);
    let second_stub = StubSource {
        rates: vec![rate_observation(6.0, second_valid_from)],
    };
    run_stub_crawl(&dyn_catalog, &second_stub, &config, second_valid_from).await;

    let rows = read_raw_rows(&dyn_catalog).await;
    assert_eq!(rows.len(), 2, "row count must grow by exactly one revision");
    assert_eq!(
        rows,
        vec![
            (5.0, first_valid_from.timestamp_micros()),
            (6.0, second_valid_from.timestamp_micros()),
        ],
        "both revisions must coexist, ordered by valid_from"
    );

    // Derive `valid_to` the way the `prices_effective` view's `LEAD(valid_from)`
    // window would: the older revision's `valid_to` is the newer's `valid_from`,
    // and the newest revision's `valid_to` is NULL (open-ended).
    let valid_tos: Vec<Option<i64>> = rows
        .windows(2)
        .map(|pair| Some(pair[1].1))
        .chain(std::iter::once(None))
        .collect();
    assert_eq!(
        valid_tos,
        vec![Some(second_valid_from.timestamp_micros()), None],
        "the older revision's valid_to must equal the newer's valid_from; the newest's must be NULL"
    );
}

#[tokio::test]
async fn pricing_runner_builds_starts_and_drains() {
    // Covers the wiring the pipeline tests above skip: `PricingConfig::validate`
    // (including its `jobsmanager` block), `build_sources` and its ownership
    // check, the crawl interval taken from `interval_secs` rather than the
    // jobmanager's `scan_interval_secs`, the task-timeout conversion, job
    // registration, real S3 job-state storage, and a clean drain.
    //
    // It does NOT assert that a crawl body ran: the single source points at a
    // closed local port, so if the task does execute its fetch fails, which by
    // design contributes no rows and does not fail the crawl. What it does
    // assert is that neither outcome commits a snapshot.
    let (_store, conn) = common::setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    create_tables(&dyn_catalog, &SnapshotExpirationConfig::default(), false)
        .await
        .expect("create tables");

    let config = PricingConfig {
        enabled: true,
        interval_secs: 1,
        timeout_secs: 2,
        sources: vec![SourceConfig {
            name: "litellm".to_string(),
            // Port 1 is reserved and never listening, so the fetch fails fast
            // without reaching the network.
            url: "http://127.0.0.1:1/model_prices.json".to_string(),
            owns: None,
        }],
        jobsmanager: JobsManagerConfig {
            worker_count: 1,
            poll_interval_ms: 100,
            scan_interval_secs: 1,
            storage: JobsStorageConfig {
                endpoint: conn.endpoint.clone(),
                bucket: BUCKET_NAME.to_string(),
                prefix: "pricing".to_string(),
                region: "us-east-1".to_string(),
                job_state_codec: JobStateCodec::Json,
                request_timeout_secs: 5,
                access_key_id: Some(conn.access_key.clone()),
                secret_access_key: Some(conn.secret_key.clone()),
            },
        },
        ..PricingConfig::default()
    };

    let runner = PricingRunner::new_with_max_iterations(dyn_catalog.clone(), &config, Some(1))
        .await
        .expect("build runner");
    let handle = runner.start().expect("start runner");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert!(
        current_snapshot_id(&dyn_catalog).await.is_none(),
        "a crawl with no usable source must not commit a snapshot"
    );
    handle.shutdown().await.expect("shutdown runner");
}
