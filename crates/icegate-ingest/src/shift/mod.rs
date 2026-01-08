//! Shift operations: moving data from WAL to Iceberg.

/// Configuration for shift operations.
pub mod config;
/// Task executors for shift operations.
pub mod executor;
/// Offset helpers for tracking WAL progress.
pub mod offset;
/// Parquet metadata reader utilities for shift operations.
pub mod parquet_meta_reader;
/// Iceberg writer utilities for shift operations.
pub mod writer;

pub use config::ShiftConfig;
pub use executor::{
    COMMIT_TASK_CODE,
    PREPARE_WAL_TASK_CODE,
    SHIFT_TASK_CODE,
    CommitInput,
    Executor,
    PrepareWalInput,
    ShiftInput,
    ShiftOutput,
    WalFile,
};
pub use offset::{get_committed_offset, OFFSET_SUMMARY_KEY};
pub use parquet_meta_reader::data_files_from_parquet_paths;
pub use writer::{Writer, WrittenDataFiles};
