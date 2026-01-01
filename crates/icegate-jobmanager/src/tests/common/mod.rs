// Common test utilities

use std::sync::OnceLock;

use tracing_subscriber::{EnvFilter, fmt};

pub mod in_memory_storage;
pub mod manager_env;
pub mod minio_env;
pub mod storage_wrapper;

pub fn init_tracing() {
    static INIT: OnceLock<()> = OnceLock::new();

    let () = INIT.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("icegate_jobmanager=debug"));
        let _ = fmt().with_env_filter(filter).with_test_writer().try_init();
    });
}
