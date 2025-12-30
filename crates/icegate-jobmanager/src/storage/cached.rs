use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{Job, JobCode, JobMeta, Metrics, Storage, StorageError, StorageResult};

// Internal structure to hold the job state
#[derive(Clone)]
struct CachedJob {
    job: Option<Job>,
    // TODO(med): use TTL or LRU cache to avoid limitless memory growth
}

/// Caching wrapper around a storage backend.
pub struct CachedStorage {
    inner: Arc<dyn Storage>,
    cache: DashMap<JobCode, Arc<Mutex<CachedJob>>>,
    metrics: Metrics,
}

impl CachedStorage {
    pub fn new(inner: Arc<dyn Storage>, metrics: Metrics) -> Self {
        Self {
            inner,
            cache: DashMap::new(),
            metrics,
        }
    }

    // update_cache_if_newer updates cache only if passed job iteration is newer or equal to current in
    // cache
    fn update_cache_if_newer(&self, storage_job: &Job, cached_job: &mut CachedJob) {
        if let Some(ref current_job) = cached_job.job {
            if storage_job.iter_num() >= current_job.iter_num() {
                cached_job.job = Some(storage_job.clone());
            }
        } else {
            cached_job.job = Some(storage_job.clone());
        }
    }

    fn record_cache_hit(&self, method: &str) {
        self.metrics.record_cache_hit(method);
    }

    fn record_cache_miss(&self, method: &str) {
        self.metrics.record_cache_miss(method);
    }
}

#[async_trait]
#[allow(private_interfaces)]
#[allow(clippy::significant_drop_tightening)]
impl Storage for CachedStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        // Always check latest meta first to validate cache.
        let mut meta = self.inner.find_job_meta(job_code, cancel_token).await?;

        let cached_job = self
            .cache
            .entry(job_code.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(CachedJob {
                    job: None,
                }))
            })
            .clone();

        let mut cached_job = cached_job.lock().await;

        // If state hasn't changed - return state from cache.
        if let Some(ref job) = cached_job.job {
            if job.iter_num() == meta.iter_num && job.version() == meta.version {
                self.record_cache_hit("get_job");
                debug!(
                    "Get job '{}' from cache (id: {}, iter: {}, version: {}, tasks: {:?})",
                    job.code(),
                    job.id(),
                    job.iter_num(),
                    job.version(),
                    job.tasks_as_string()
                );
                return Ok(job.clone());
            }
        }

        self.record_cache_miss("get_job");

        // Keep job lock while fetching to avoid get/save races.
        let job = loop {
            match self.inner.get_job_by_meta(&meta, cancel_token).await {
                Ok(job) => break job,
                Err(e) if e.is_conflict() => {
                    meta = self.inner.find_job_meta(job_code, cancel_token).await?;
                },
                Err(e) => return Err(e),
            }
        };

        cached_job.job = Some(job.clone());
        Ok(job)
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn get_job_by_meta(&self, meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let cached_job = self
            .cache
            .entry(meta.code.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(CachedJob {
                    job: None,
                }))
            })
            .clone();

        let mut cached_job = cached_job.lock().await;

        if let Some(ref job) = cached_job.job {
            if job.iter_num() == meta.iter_num && job.version() == meta.version {
                self.record_cache_hit("get_job_by_meta");
                debug!(
                    "Get job '{}' from cache (id: {}, iter: {}, version: {}, tasks: {:?})",
                    job.code(),
                    job.id(),
                    job.iter_num(),
                    job.version(),
                    job.tasks_as_string()
                );
                return Ok(job.clone());
            }
        }
        self.record_cache_miss("get_job_by_meta");

        let job = match self.inner.get_job_by_meta(meta, cancel_token).await {
            Ok(job) => job,
            Err(e) if e.is_conflict() => {
                // Invalidate cache so next read fetches fresh state
                cached_job.job = None;
                return Err(e);
            },
            Err(e) => return Err(e),
        };

        self.update_cache_if_newer(&job, &mut cached_job);
        Ok(job)
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let cached_job = self
            .cache
            .entry(job.code().clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(CachedJob {
                    job: None,
                }))
            })
            .clone();

        let mut cached_job = cached_job.lock().await;

        match self.inner.save_job(job, cancel_token).await {
            Ok(()) => {
                cached_job.job = Some(job.clone());
                debug!(
                    "Job '{}' saved to storage and cache (id: {}, iter: {}, version: {}, tasks: {:?})",
                    job.code(),
                    job.id(),
                    job.iter_num(),
                    job.version(),
                    job.tasks_as_string()
                );
                Ok(())
            },
            Err(e) if e.is_conflict() => {
                // Invalidate cache so next get_job fetches fresh state from S3
                cached_job.job = None;
                Err(e)
            },
            Err(e) => Err(e),
        }
    }
}
