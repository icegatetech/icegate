//! Shift operations for moving data from queue to Iceberg tables.
//!
//! This module provides functionality to continuously read data from the queue
//! and write it to Iceberg tables with exactly-once delivery guarantees.

pub mod config;
pub mod offset;
pub mod processor;
pub mod writer;

pub use config::ShiftConfig;
pub use processor::ShiftProcessor;
