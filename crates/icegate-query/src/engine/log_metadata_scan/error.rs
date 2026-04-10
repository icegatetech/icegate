//! Errors produced by the metadata scan path for label endpoints.

use thiserror::Error;

/// Errors returned from the Parquet metadata scan for `/labels` and
/// `/label_values`.
#[derive(Debug, Error)]
pub enum MetadataScanError {
    /// Iceberg planning or scan-task construction failed.
    #[error("iceberg planning failed: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// Parquet decoding error (footer, page index, or data page).
    ///
    /// The offending file path is carried in the surrounding tracing span
    /// (`file` field) rather than in the error value itself, which keeps
    /// the error lightweight and avoids redundant string allocations on
    /// every hot-path call site.
    #[error("parquet read failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// The Parquet file did not contain an expected column or leaf path.
    #[error("schema mismatch: {0}")]
    Schema(String),
}

impl From<MetadataScanError> for crate::error::QueryError {
    fn from(err: MetadataScanError) -> Self {
        match err {
            MetadataScanError::Iceberg(e) => Self::Iceberg(e),
            MetadataScanError::Parquet(e) => Self::Internal(format!("parquet read failed: {e}")),
            MetadataScanError::Schema(msg) => Self::Internal(format!("schema mismatch: {msg}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MetadataScanError;

    #[test]
    fn metadata_scan_error_displays_parquet_variant() {
        let err = MetadataScanError::Parquet(parquet::errors::ParquetError::General("boom".to_string()));
        let s = format!("{err}");
        assert!(s.contains("boom"));
    }
}
