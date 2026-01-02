use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{
    Job, JobCode,
    storage::{JobMeta, Storage, StorageResult},
};

/// `CountingStorage` wraps Storage and tracks `save_job` calls.
pub struct CountingStorage {
    inner: Arc<dyn Storage>,
    put_attempts: AtomicU64,
    put_successes: AtomicU64,
    list_and_get_successes: AtomicU64,
}

impl CountingStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            put_attempts: AtomicU64::new(0),
            put_successes: AtomicU64::new(0),
            list_and_get_successes: AtomicU64::new(0),
        }
    }

    pub fn put_attempts(&self) -> u64 {
        self.put_attempts.load(Ordering::SeqCst)
    }

    pub fn put_successes(&self) -> u64 {
        self.put_successes.load(Ordering::SeqCst)
    }

    pub fn list_and_get_successes(&self) -> u64 {
        self.list_and_get_successes.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Storage for CountingStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        let result = self.inner.get_job(job_code, cancel_token).await;
        if result.is_ok() {
            self.list_and_get_successes.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        self.put_attempts.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            "CountingStorage::save_job attempt {} - job: {}, iter: {}, version: {}",
            self.put_attempts.load(Ordering::SeqCst),
            job.code(),
            job.iter_num(),
            job.version()
        );
        let result = self.inner.save_job(job, cancel_token).await;
        if result.is_ok() {
            self.put_successes.fetch_add(1, Ordering::SeqCst);
            tracing::info!(
                "CountingStorage::save_job SUCCESS {} - job: {}, iter: {}, version: {}",
                self.put_successes.load(Ordering::SeqCst),
                job.code(),
                job.iter_num(),
                job.version()
            );
        } else {
            tracing::info!(
                "CountingStorage::save_job CONFLICT - job: {}, iter: {}, version: {}",
                job.code(),
                job.iter_num(),
                job.version()
            );
        }
        result
    }
}
