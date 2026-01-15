#![allow(missing_docs)]
#![allow(dead_code)]
#![allow(clippy::redundant_pub_crate)]

pub(crate) mod core;
mod execution;
mod infra;
mod storage;

// Public compatibility modules
pub mod error {
    pub use crate::core::error::{Error, Result};
}
pub mod registry {
    pub use crate::core::registry::TaskExecutorFn;
}
pub mod s3_storage {
    pub use crate::storage::s3::{JobStateCodecKind, S3Storage, S3StorageConfig};
}

// Re-exports
pub use error::Error;
pub(crate) use storage::{JobMeta, Storage, StorageError, StorageResult};

pub(crate) use crate::core::error::{InternalError, JobError};
pub(crate) use crate::core::job::Job;
pub use crate::core::job::{JobCode, JobDefinition, JobStatus, TaskLimits};
pub use crate::core::registry::JobRegistry;
pub(crate) use crate::core::task::Task;
pub use crate::core::task::{ImmutableTask, TaskCode, TaskDefinition, TaskStatus};
pub use crate::execution::job_manager::JobManager;
pub use crate::execution::jobs_manager::{JobsManager, JobsManagerConfig, JobsManagerHandle};
pub(crate) use crate::execution::worker::Worker;
pub use crate::execution::worker::WorkerConfig;
pub use crate::infra::metrics::Metrics;
pub(crate) use crate::infra::retrier::Retrier;
pub use crate::infra::retrier::RetrierConfig;
pub use crate::storage::JobDefinitionRegistry;
pub use crate::storage::cached::CachedStorage;
pub use crate::storage::in_memory::InMemoryStorage;
pub use crate::storage::s3::S3Storage;

// Integration tests
#[cfg(test)]
mod tests;
