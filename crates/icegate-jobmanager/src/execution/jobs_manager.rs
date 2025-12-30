use std::sync::Arc;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{Error, InternalError, JobRegistry, Metrics, Storage, Worker, WorkerConfig};

#[derive(Clone)]
pub struct JobsManagerConfig {
    pub worker_count: usize,
    pub worker_config: WorkerConfig,
}

impl Default for JobsManagerConfig {
    fn default() -> Self {
        Self {
            worker_count: 1,
            worker_config: WorkerConfig::default(),
        }
    }
}

pub struct JobsManager {
    job_registry: Arc<JobRegistry>,
    storage: Arc<dyn Storage>,
    config: JobsManagerConfig,
    metrics: Metrics,
}

/// Handle for controlling a running `JobsManager`.
pub struct JobsManagerHandle {
    cancel_token: CancellationToken,
    join_set: JoinSet<Result<(), InternalError>>,
}

impl JobsManagerHandle {
    /// Gracefully stop workers and wait for completion.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.cancel_token.cancel();
        self.wait().await
    }

    /// Force-abort worker tasks.
    pub fn abort(&mut self) {
        self.cancel_token.cancel();
        self.join_set.abort_all();
    }

    async fn wait(&mut self) -> Result<(), Error> {
        while let Some(result) = self.join_set.join_next().await {
            #[allow(clippy::match_same_arms)]
            match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    // Error is already logged inside the worker task.
                }
                Err(e) => {
                    error!("Worker panicked: {}", e);
                }
            }
        }

        Ok(())
    }
}

impl Drop for JobsManagerHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

impl JobsManager {
    // TODO(med): we need to allocate an asynchronous API for managing and configuring jobs. So the
    // client will be able to change job settings without restarting. TODO(low): consider reducing
    // the number of parameters for manager startup
    pub fn new(
        storage: Arc<dyn Storage>,
        config: JobsManagerConfig,
        job_registry: Arc<JobRegistry>,
        metrics: Metrics,
    ) -> Result<Self, Error> {
        Ok(Self {
            job_registry,
            storage,
            config,
            metrics,
        })
    }

    /// Start worker tasks and return a handle for lifecycle control.
    pub fn start(&self) -> Result<JobsManagerHandle, Error> {
        info!("Starting {} workers", self.config.worker_count);

        let cancel_token = CancellationToken::new();
        let mut join_set = JoinSet::new();

        // TODO(med): dynamic worker count - reduce workers when there's little work to minimize storage
        // requests
        for i in 0..self.config.worker_count {
            let worker = Worker::new(
                Arc::clone(&self.job_registry),
                Arc::clone(&self.storage),
                self.config.worker_config.clone(),
                self.metrics.clone(),
            );

            let token = cancel_token.clone();
            let worker_id = i;

            join_set.spawn(async move {
                // TODO(high): decide what to do with the panic, now the worker is dying.
                if let Err(e) = worker.start(token).await {
                    tracing::error!("Worker {} stopped with error: {}", worker_id, e);
                    Err(e)
                } else {
                    Ok(())
                }
            });
        }

        Ok(JobsManagerHandle { cancel_token, join_set })
    }
}
