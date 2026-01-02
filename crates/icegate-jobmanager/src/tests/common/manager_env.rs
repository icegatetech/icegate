use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;

use crate::{JobCode, JobDefinition, JobRegistry, JobsManager, JobsManagerConfig, JobsManagerHandle, Metrics, Storage};

/// `ManagerEnv` manages `JobsManager` lifecycle for integration tests
pub struct ManagerEnv {
    storage: Arc<dyn Storage>,
    job_registry: HashMap<JobCode, JobDefinition>,
    manager_handle: Option<JobsManagerHandle>,
}

impl ManagerEnv {
    /// Create a new `ManagerEnv`
    pub fn new(
        storage: Arc<dyn Storage>,
        config: JobsManagerConfig,
        job_registry_handle: Arc<JobRegistry>,
        job_registry: Vec<JobDefinition>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let manager = JobsManager::new(storage.clone(), config, job_registry_handle, Metrics::new_disabled())?;
        let manager_handle = manager.start()?;

        // Store job definitions for later checking
        let job_defs_map: HashMap<_, _> = job_registry.into_iter().map(|def| (def.code().clone(), def)).collect();

        Ok(Self {
            storage,
            job_registry: job_defs_map,
            manager_handle: Some(manager_handle),
        })
    }

    /// Wait for all jobs with iteration limits to complete
    pub async fn wait_for_all_jobs_completion(&self, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
        let start = tokio::time::Instant::now();
        let cancel_token = CancellationToken::new();

        loop {
            if start.elapsed() > timeout {
                return Err("timeout waiting for jobs to complete".into());
            }

            // Check all registered jobs
            let mut all_done = true;

            for (job_code, job_def) in &self.job_registry {
                // Skip unlimited jobs (max_iterations == 0)
                if job_def.max_iterations() == 0 {
                    continue;
                }

                match self.storage.get_job(job_code, &cancel_token).await {
                    Ok(job) => {
                        // Job is not done if:
                        // 1. Not processed yet, OR
                        // 2. Hasn't reached the iteration limit
                        if !job.is_processed() || job.iter_num() < job_def.max_iterations() {
                            all_done = false;
                            break;
                        }
                    }
                    Err(_) => {
                        // Job doesn't exist yet or error reading it
                        all_done = false;
                        break;
                    }
                }
            }

            if all_done {
                tracing::info!("All jobs completed");
                return Ok(());
            }

            // Wait a bit before checking again
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stop the manager
    pub async fn stop(&mut self) {
        if let Some(handle) = self.manager_handle.take() {
            let _ = handle.shutdown().await;
        }
    }

    /// Get the storage instance
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }
}

// Automatically stop manager when dropped
impl Drop for ManagerEnv {
    fn drop(&mut self) {
        if let Some(mut handle) = self.manager_handle.take() {
            handle.abort();
        }
    }
}
