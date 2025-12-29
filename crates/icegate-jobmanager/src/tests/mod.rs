// Integration tests module
// These tests are kept in src/ rather than tests/ to access pub(crate) types like Job

mod common;

mod cache_invalidation_test;
mod concurrent_workers_test;
mod deadline_expiry_test;
mod dynamic_task_test;
mod job_iterations_test;
mod shutdown_test;
mod simple_job_test;
mod task_failure_test;
mod two_jobs_test;
