//! Error types for common operations.

use std::io;

/// Result type alias for common operations.
pub type Result<T> = std::result::Result<T, CommonError>;

/// Errors that can occur in common operations.
#[derive(Debug, thiserror::Error)]
pub enum CommonError {
    /// Configuration loading/parsing error (generic).
    #[error("configuration error: {0}")]
    Config(String),

    /// TOML parsing error.
    #[error("toml parse error: {0}")]
    Toml(#[from] toml::de::Error),

    /// YAML parsing error.
    #[error("yaml parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// Underlying Iceberg error.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// Generic write-pipeline error (cancellation, overflow, partition split,
    /// or an upstream error bridged across a crate boundary).
    #[error("write error: {0}")]
    Write(String),

    /// Compaction read-path error: a manifest-scan decode mismatch, such as a
    /// sort-key column whose `lower_bounds`/`upper_bounds` `Datum` carries a
    /// `PrimitiveLiteral` variant that disagrees with the column's declared
    /// Iceberg primitive type (e.g. a `Timestamp` column whose bound is not a
    /// `Long`).
    #[error("compaction read error: {0}")]
    CompactRead(String),

    /// I/O error (file reading).
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// Object store error.
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
}
