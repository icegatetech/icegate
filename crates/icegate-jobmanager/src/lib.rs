#![allow(missing_docs)]
#![allow(dead_code)]

mod core;
mod execution;
mod infra;
pub mod storage;

// Public compatibility modules
pub mod error {
    pub use crate::core::error::*;
}
pub mod job {
    pub use crate::core::job::*;
}
pub mod task {
    pub use crate::core::task::*;
}
pub mod registry {
    pub use crate::core::registry::*;
}
pub mod metrics {
    pub use crate::infra::metrics::*;
}
pub mod job_manager {
    pub use crate::execution::job_manager::*;
}
pub mod jobs_manager {
    pub use crate::execution::jobs_manager::*;
}
pub mod worker {
    pub use crate::execution::worker::*;
}
pub mod retrier {
    pub use crate::infra::retrier::*;
}
pub mod cached_storage {
    pub use crate::storage::cached::*;
}
pub mod s3_storage {
    pub use crate::storage::s3::*;
}

// Re-exports
pub use cached_storage::CachedStorage;
pub use error::Error;
pub(crate) use job::Job;
pub use job::{JobCode, JobDefinition, JobStatus};
pub use job_manager::JobManager;
pub use jobs_manager::{JobsManager, JobsManagerConfig, JobsManagerHandle};
pub use metrics::Metrics;
pub use registry::JobRegistry;
pub(crate) use retrier::Retrier;
pub use retrier::RetrierConfig;
pub use s3_storage::S3Storage;
pub use storage::JobDefinitionRegistry;
pub(crate) use storage::{JobMeta, Storage, StorageError, StorageResult};
pub(crate) use task::Task;
pub use task::{ImmutableTask, TaskCode, TaskDefinition, TaskStatus};
pub(crate) use worker::Worker;
pub use worker::WorkerConfig;

// Integration tests (moved from tests/ to have access to pub(crate) types)
#[cfg(test)]
mod tests;
