use std::{collections::HashMap, sync::Arc};

use chrono::Duration as ChronoDuration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::in_memory_storage::InMemoryStorage;
use crate::{CachedStorage, Job, JobCode, Metrics, Storage, Task, TaskCode, TaskDefinition};

// TestCacheInvalidation verifies that CachedStorage avoids redundant S3 calls on cache hit.
#[tokio::test]
async fn test_cache_invalidation() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let storage = Arc::new(InMemoryStorage::new());
    let cached_storage = Arc::new(CachedStorage::new(
        storage.clone() as Arc<dyn Storage>,
        Metrics::new_disabled(),
    ));
    let cancel_token = CancellationToken::new();
    let worker_id = Uuid::from_u128(1);

    let task = Task::new(
        worker_id,
        &TaskDefinition::new(TaskCode::from("cache_task"), Vec::new(), ChronoDuration::seconds(5))?,
    );

    let job_code = JobCode::new("test_cache_job");

    let mut job = Job::new(job_code.clone(), vec![task], HashMap::new(), worker_id, Some(1), None);
    job.work(&worker_id)?;

    storage.save_job(&mut job, &cancel_token).await?;
    assert_eq!(storage.version(), 1);

    let job_from_cache = cached_storage.get_job(&job_code, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 1);
    assert_eq!(storage.get_by_meta_calls(), 1);
    assert_eq!(storage.version(), 1);
    assert_eq!(job_from_cache.version(), storage.version().to_string());

    cached_storage.save_job(&mut job, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 1);
    assert_eq!(storage.get_by_meta_calls(), 1);
    assert_eq!(storage.version(), 2);
    assert_eq!(job.version(), storage.version().to_string());

    let job_from_cache = cached_storage.get_job(&job_code, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 2);
    assert_eq!(storage.get_by_meta_calls(), 1);
    assert_eq!(storage.version(), 2);
    assert_eq!(job_from_cache.version(), storage.version().to_string());

    storage.save_job(&mut job, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 2);
    assert_eq!(storage.get_by_meta_calls(), 1);
    assert_eq!(storage.version(), 3);
    assert_eq!(job.version(), storage.version().to_string());

    let job_from_cache = cached_storage.get_job(&job_code, &cancel_token).await?;
    assert_eq!(storage.find_meta_calls(), 3);
    assert_eq!(storage.get_by_meta_calls(), 2);
    assert_eq!(storage.version(), 3);
    assert_eq!(job_from_cache.version(), storage.version().to_string());

    Ok(())
}
